# Data race in `range_scan_orchestrator` — reproduction

This fork reproduces a **data race** in the KV range-scan orchestrator of
`couchbase-cxx-client`, demonstrated with **ThreadSanitizer**.

| Branch | Contents |
|--------|----------|
| [`reproduction`](../../tree/reproduction) (this branch) | upstream `main` + a ThreadSanitizer harness that makes the race fire **deterministically** — build & run it to see the report |
| [`main`](../../tree/main) | upstream `main` + **only the fix** (one commit) — mergeable, no harness |

The harness is test scaffolding only; the bug and the fix live entirely in stock
`next_item()` (see below). Locking the read on this branch removes the race too
(verified) — that single lock is exactly what `main` contains.

## The bug

`couchbase::core::range_scan_orchestrator_impl::next_item()`
(`core/range_scan_orchestrator.cxx`) reads the `streams_` map without holding
`stream_map_mutex_`:

```cpp
if (streams_.empty() || cancelled_) {   // <-- unlocked read of streams_
```

while the io thread mutates `streams_` **under** that lock — erase on stream
completion:

```cpp
{
  const std::lock_guard<std::mutex> lock{ self->stream_map_mutex_ };
  self->streams_.erase(signal.vbucket_id);
}
```

and insert in `start_streams()`. Every access to `streams_` takes
`stream_map_mutex_` **except** that one read. That is a data race (C++ UB).
(`cancelled_` is `std::atomic<bool>`, so the race is purely on `streams_`.)

In production this surfaces as an intermittent KV range scan returning one
document short (e.g. 99 of 100) under load, even with `consistent_with` set.

### Why it is rare (and why the harness exists)

A single consumer reads `streams_` ~once per returned document, while the io
thread touches it thousands of times per scan, so the consumer's unlocked read is
almost always evicted from ThreadSanitizer's shadow memory before a write checks
against it — exactly why this is a rare production flake. To make it
**deterministic**, the harness adds a helper thread that performs the *same*
unlocked read continuously (both the real `next_item()` read and the helper go
through one `streams_empty()` accessor). The bug and the fix live entirely in
stock `next_item()`; the helper is test scaffolding only.

## The fix

Take `stream_map_mutex_` for the read, matching every other access to `streams_`.
On `main` the shared `streams_empty()` accessor is locked:

```cpp
auto streams_empty() -> bool
{
  const std::lock_guard<std::mutex> lock{ stream_map_mutex_ };
  return streams_.empty();
}
```

## Reproduce

Requirements: a C++ toolchain with ThreadSanitizer (clang or gcc), CMake, and a
**running Couchbase Server** (tested with 8.0 Enterprise) reachable at
`couchbase://127.0.0.1` with user `Administrator` / password `password` and a
bucket named **`store`**.

```bash
git clone --recurse-submodules https://github.com/JesusTheHun/couchbase-cxx-client
cd couchbase-cxx-client
git checkout reproduction          # RED  (use 'main' for GREEN)
git submodule update --init --recursive

cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DENABLE_SANITIZER_THREAD=ON \
  -DCOUCHBASE_CXX_CLIENT_BUILD_TESTS=OFF \
  -DCOUCHBASE_CXX_CLIENT_BUILD_TOOLS=OFF \
  -DCOUCHBASE_CXX_CLIENT_BUILD_DOCS=OFF \
  -DCOUCHBASE_CXX_CLIENT_BUILD_EXAMPLES=ON
cmake --build build --target minimal -j

TSAN_OPTIONS="halt_on_error=1" ./build/examples/minimal
```

The example seeds 100 docs into `store._default._default` and loops range scans
from several threads (env overrides: `REPRO_THREADS`, `REPRO_ITER`, `REPRO_SEED`).

### Expected

ThreadSanitizer prints `WARNING: ThreadSanitizer: data race`, with the write in
`streams_.erase` (holding `stream_map_mutex_`) and the previous read being the
unlocked `streams_.empty()`, on the same `streams_` heap block. The process exits
non-zero. Reproduced 3/3 in testing.

To confirm the fix: lock the `streams_empty()` read on this branch (the change on
[`main`](../../tree/main)) and rerun — ThreadSanitizer reports no race and every
scan returns the full document count (verified, 0 races over 120 scans).

> Note on the build: stock `enable_sanitizers` adds `-fsanitize` only as an
> `INTERFACE` property (i.e. to consumers, not to the library's own translation
> units), so the harness also adds `-fsanitize=thread` to the library target —
> otherwise the orchestrator is invisible to ThreadSanitizer.

## Affected version

The race is present in `core/range_scan_orchestrator.cxx` on `main`
(`d93d449`, 2026-06-02) and is byte-identical in tag **1.2.2** (`300daf1`), the
revision pinned by the `@couchbase/couchbase` Node.js SDK v4.6.1.

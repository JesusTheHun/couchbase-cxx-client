/*
 * TSan reproduction for the KV range-scan data race in range_scan_orchestrator.
 *
 * next_item() reads streams_.empty() WITHOUT holding stream_map_mutex_, while
 * the single io thread mutates streams_ (insert in start_streams / erase on
 * stream completion) UNDER the lock. That unsynchronized access is a data race;
 * under the right interleaving the consumer takes the early-exit branch and
 * discards a still-buffered item (the intermittent 99/100 drop).
 *
 * Many concurrent consumer threads each drive their own scan; the shared io
 * thread is therefore almost always churning some orchestrator's streams_ map
 * while that scan's consumer thread reads streams_.empty() -> the conflicting
 * accesses coincide and TSan reports the race.
 *
 * Build with -fsanitize=thread, run against a live cluster.
 */
#include <couchbase/cluster.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>

#include <atomic>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

static constexpr auto connection_string{ "couchbase://127.0.0.1" };
static constexpr auto username{ "Administrator" };
static constexpr auto password{ "password" };
static constexpr auto bucket_name{ "store" };
static constexpr auto scope_name{ couchbase::scope::default_name };
static constexpr auto collection_name{ couchbase::collection::default_name };

static int
env_int(const char* name, int fallback)
{
  if (const char* v = std::getenv(name)) {
    return std::atoi(v);
  }
  return fallback;
}

int
main()
{
  const int seed_count = env_int("REPRO_SEED", 100);
  const int scan_runs = env_int("REPRO_ITER", 40);
  const int threads = env_int("REPRO_THREADS", 8);

  auto options = couchbase::cluster_options(username, password);
  auto [connect_err, cluster] = couchbase::cluster::connect(connection_string, options).get();
  if (connect_err) {
    std::cerr << "connect failed: " << connect_err.ec().message() << "\n";
    return 1;
  }
  auto collection = cluster.bucket(bucket_name).scope(scope_name).collection(collection_name);

  std::cerr << "seeding " << seed_count << " docs...\n";
  couchbase::mutation_state state;
  for (int i = 1; i <= seed_count; ++i) {
    const std::string id = "doc::" + std::to_string(i);
    const tao::json::value doc{ { "id", id } };
    auto [err, resp] = collection.upsert(id, doc, {}).get();
    if (err.ec()) {
      std::cerr << "upsert " << id << " failed: " << err.ec().message() << "\n";
      return 1;
    }
    state.add(resp);
  }
  std::cerr << "mutation tokens: " << state.tokens().size() << "\n";
  std::cerr << "running " << threads << " consumer threads x " << scan_runs << " scans...\n";

  std::atomic<int> total_drops{ 0 };

  auto worker = [&]() {
    for (int run = 0; run < scan_runs; ++run) {
      // batch_byte_limit(1) + batch_item_limit(1): force many continue
      // round-trips and stream create/erase churn -> maximize the streams_
      // race window.
      auto [scan_err, result] =
        collection
          .scan(couchbase::range_scan{},
                couchbase::scan_options{}
                  .batch_byte_limit(1)
                  .batch_item_limit(1)
                  .consistent_with(state))
          .get();
      if (scan_err) {
        continue;
      }
      int count = 0;
      for (;;) {
        auto [item_err, item] = result.next().get();
        if (item_err || !item.has_value()) {
          break;
        }
        ++count;
      }
      if (count != seed_count) {
        total_drops.fetch_add(1);
      }
    }
  };

  std::vector<std::thread> pool;
  for (int t = 0; t < threads; ++t) {
    pool.emplace_back(worker);
  }
  for (auto& th : pool) {
    th.join();
  }

  std::cout << "TOTAL DROPS: " << total_drops.load() << " / " << (threads * scan_runs) << "\n";

  for (int i = 1; i <= seed_count; ++i) {
    collection.remove("doc::" + std::to_string(i), {}).get();
  }
  cluster.close().get();
  return 0;
}

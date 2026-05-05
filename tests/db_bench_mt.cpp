#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include "db.h"
#include "options.h"

using namespace minidb;

namespace {

struct Config {
    int threads = 4;
    int ops_per_thread = 5000;
    int value_size = 100;
    int write_buffer_size = 256 * 1024;
    std::string db_name = "./test_results/db_bench_mt_default/db";
};

bool StartsWith(const std::string& text, const std::string& prefix) {
    return text.rfind(prefix, 0) == 0;
}

bool ParseIntArg(const std::string& arg, const std::string& key, int* out) {
    if (!StartsWith(arg, key)) return false;
    *out = std::stoi(arg.substr(key.size()));
    return true;
}

bool ParseStringArg(const std::string& arg, const std::string& key, std::string* out) {
    if (!StartsWith(arg, key)) return false;
    *out = arg.substr(key.size());
    return true;
}

bool ParseArgs(int argc, char** argv, Config* cfg) {
    for (int i = 1; i < argc; ++i) {
        std::string arg(argv[i]);
        if (ParseIntArg(arg, "--threads=", &cfg->threads)) continue;
        if (ParseIntArg(arg, "--ops_per_thread=", &cfg->ops_per_thread)) continue;
        if (ParseIntArg(arg, "--value_size=", &cfg->value_size)) continue;
        if (ParseIntArg(arg, "--write_buffer_size=", &cfg->write_buffer_size)) continue;
        if (ParseStringArg(arg, "--db_name=", &cfg->db_name)) continue;
        std::cerr << "Unknown arg: " << arg << "\n";
        return false;
    }
    return cfg->threads > 0 && cfg->ops_per_thread > 0 &&
           cfg->value_size > 0 && cfg->write_buffer_size > 0;
}

double PercentileMicros(const std::vector<double>& sorted_micros, double p) {
    if (sorted_micros.empty()) return 0.0;
    double pos = (p / 100.0) * static_cast<double>(sorted_micros.size() - 1);
    size_t lo = static_cast<size_t>(std::floor(pos));
    size_t hi = static_cast<size_t>(std::ceil(pos));
    if (lo == hi) return sorted_micros[lo];
    double w = pos - static_cast<double>(lo);
    return sorted_micros[lo] * (1.0 - w) + sorted_micros[hi] * w;
}

std::string FixedKey(int tid, int i) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "t%03d_%09d", tid, i);
    std::string s(buf);
    if (s.size() < 16) s.append(16 - s.size(), '_');
    if (s.size() > 16) s.resize(16);
    return s;
}

std::string FixedValue(int tid, int i, int value_size) {
    std::string prefix = "v" + std::to_string(tid) + "_" + std::to_string(i) + "_";
    if (static_cast<int>(prefix.size()) >= value_size) return prefix.substr(0, static_cast<size_t>(value_size));
    return prefix + std::string(static_cast<size_t>(value_size) - prefix.size(), 'x');
}

}  // namespace

int main(int argc, char** argv) {
    Config cfg;
    if (!ParseArgs(argc, argv, &cfg)) {
        std::cerr << "Usage: " << argv[0]
                  << " --threads=N --ops_per_thread=N --value_size=N"
                  << " --write_buffer_size=N --db_name=PATH\n";
        return 1;
    }

    Options options;
    options.create_if_missing = true;
    options.write_buffer_size = static_cast<size_t>(cfg.write_buffer_size);

    DB* db = nullptr;
    Status s = DB::Open(options, cfg.db_name, &db);
    if (!s.ok()) {
        std::cerr << "DB::Open failed: " << s.ToString() << "\n";
        return 1;
    }

    std::vector<std::vector<double>> per_thread_lat(static_cast<size_t>(cfg.threads));
    for (int i = 0; i < cfg.threads; ++i) {
        per_thread_lat[static_cast<size_t>(i)].reserve(static_cast<size_t>(cfg.ops_per_thread));
    }

    auto start = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
    workers.reserve(static_cast<size_t>(cfg.threads));
    for (int tid = 0; tid < cfg.threads; ++tid) {
        workers.emplace_back([&, tid]() {
            WriteOptions wo;
            for (int i = 0; i < cfg.ops_per_thread; ++i) {
                std::string key = FixedKey(tid, i);
                std::string value = FixedValue(tid, i, cfg.value_size);
                auto t0 = std::chrono::steady_clock::now();
                Status ws = db->Put(wo, Slice(key), Slice(value));
                auto t1 = std::chrono::steady_clock::now();
                if (!ws.ok()) {
                    std::cerr << "Put failed: " << ws.ToString() << "\n";
                    continue;
                }
                double micros = std::chrono::duration<double, std::micro>(t1 - t0).count();
                per_thread_lat[static_cast<size_t>(tid)].push_back(micros);
            }
        });
    }
    for (auto& t : workers) t.join();
    auto end = std::chrono::steady_clock::now();

    std::vector<double> all_lat;
    size_t total_ops = 0;
    for (const auto& v : per_thread_lat) {
        total_ops += v.size();
        all_lat.insert(all_lat.end(), v.begin(), v.end());
    }
    std::sort(all_lat.begin(), all_lat.end());

    double elapsed_s = std::chrono::duration<double>(end - start).count();
    double ops_per_sec = (elapsed_s > 0.0) ? (static_cast<double>(total_ops) / elapsed_s) : 0.0;
    double p50 = PercentileMicros(all_lat, 50.0);
    double p95 = PercentileMicros(all_lat, 95.0);
    double p99 = PercentileMicros(all_lat, 99.0);

    std::cout << "=== Mini-LevelDB MT Benchmark ===\n";
    std::cout << "threads=" << cfg.threads
              << ", ops_per_thread=" << cfg.ops_per_thread
              << ", value_size=" << cfg.value_size
              << ", write_buffer_size=" << cfg.write_buffer_size
              << ", total_ops=" << total_ops << "\n";
    std::cout << "elapsed_s=" << elapsed_s
              << ", ops_per_sec=" << ops_per_sec << "\n";
    std::cout << "latency_us: p50=" << p50
              << ", p95=" << p95
              << ", p99=" << p99 << "\n";

    delete db;
    return 0;
}

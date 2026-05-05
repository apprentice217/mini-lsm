#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>
#include "db.h"
#include "db_impl.h"
#include "options.h"

using namespace minidb;

namespace {

struct Config {
    int num_entries = 20000;
    int value_size = 100;
    int churn_rounds = 3;
    int hot_key_space = 2000;
    std::string base_dir = "./test_results/compaction_ab_default/ab";
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
        if (ParseIntArg(arg, "--num_entries=", &cfg->num_entries)) continue;
        if (ParseIntArg(arg, "--value_size=", &cfg->value_size)) continue;
        if (ParseIntArg(arg, "--churn_rounds=", &cfg->churn_rounds)) continue;
        if (ParseIntArg(arg, "--hot_key_space=", &cfg->hot_key_space)) continue;
        if (ParseStringArg(arg, "--base_dir=", &cfg->base_dir)) continue;
        std::cerr << "Unknown arg: " << arg << "\n";
        return false;
    }
    return cfg->num_entries > 0 && cfg->value_size > 0 &&
           cfg->churn_rounds >= 0 && cfg->hot_key_space > 0;
}

std::string FixedKey(int i) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "k_%09d", i);
    std::string s(buf);
    if (s.size() < 16) s.append(16 - s.size(), '_');
    if (s.size() > 16) s.resize(16);
    return s;
}

std::string FixedValue(int i, int value_size) {
    std::string v = "v_" + std::to_string(i) + "_";
    if (static_cast<int>(v.size()) >= value_size) return v.substr(0, static_cast<size_t>(value_size));
    return v + std::string(static_cast<size_t>(value_size - static_cast<int>(v.size())), 'x');
}

struct DirStats {
    uint64_t sst_files = 0;
    uint64_t log_files = 0;
    uint64_t total_bytes = 0;
};

DirStats ScanDir(const std::string& path) {
    DirStats st;
    std::error_code ec;
    for (const auto& entry : std::filesystem::directory_iterator(path, ec)) {
        if (ec) break;
        if (!entry.is_regular_file()) continue;
        auto ext = entry.path().extension().string();
        if (ext == ".sst") ++st.sst_files;
        if (ext == ".log") ++st.log_files;
        st.total_bytes += static_cast<uint64_t>(entry.file_size());
    }
    return st;
}

struct Result {
    double write_seconds = 0.0;
    double read_seconds = 0.0;
    uint64_t found = 0;
    uint64_t total_writes = 0;
    DirStats stats;
    std::vector<uint64_t> level_file_counts;
    std::vector<uint64_t> level_total_bytes;
};

void WaitForCompactionDrain(DB* db, bool disable_auto_compaction) {
    if (disable_auto_compaction) return;
    DBImpl* impl = dynamic_cast<DBImpl*>(db);
    if (impl == nullptr) return;

    // 避免“写入刚结束就采样”导致 compaction_on 尚未把 L0 backlog 处理完。
    // 等待上限较短，时间会计入 write_s，反映端到端写开销。
    const int kMaxWaitMs = 2500;
    int waited_ms = 0;
    uint64_t prev_l0_files = UINT64_MAX;
    int stable_rounds = 0;
    while (waited_ms < kMaxWaitMs) {
        std::vector<uint64_t> files;
        std::vector<uint64_t> bytes;
        impl->GetLevelFileStats(&files, &bytes);
        const uint64_t l0_files = files.empty() ? 0 : files[0];
        if (l0_files == 0) return;
        if (l0_files == prev_l0_files) {
            ++stable_rounds;
            if (stable_rounds >= 4) return;
        } else {
            stable_rounds = 0;
        }
        prev_l0_files = l0_files;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        waited_ms += 50;
    }
}

Result RunCase(const std::string& db_path, bool disable_auto_compaction, int num_entries,
               int value_size, int churn_rounds, int hot_key_space) {
    std::error_code ec;
    std::filesystem::remove_all(db_path, ec);

    Options options;
    options.create_if_missing = true;
    options.disable_auto_compaction = disable_auto_compaction;
    options.write_buffer_size = 64 * 1024;
    options.l0_compaction_trigger = 4;

    DB* db = nullptr;
    Status s = DB::Open(options, db_path, &db);
    if (!s.ok()) {
        std::cerr << "DB::Open failed: " << s.ToString() << "\n";
        return {};
    }

    WriteOptions wo;
    auto w0 = std::chrono::steady_clock::now();
    for (int i = 0; i < num_entries; ++i) {
        std::string key = FixedKey(i);
        std::string value = FixedValue(i, value_size);
        Status ws = db->Put(wo, Slice(key), Slice(value));
        if (!ws.ok()) std::cerr << "Put failed: " << ws.ToString() << "\n";
    }
    uint64_t total_writes = static_cast<uint64_t>(num_entries);

    // Churn workload: repeatedly overwrite a hot key-space to制造大量历史版本，
    // 让 compaction_on/off 在空间和读放大上的差异更加可观测。
    for (int round = 0; round < churn_rounds; ++round) {
        for (int i = 0; i < num_entries; ++i) {
            int k = i % hot_key_space;
            std::string key = FixedKey(k);
            std::string value = FixedValue((round + 1) * num_entries + i, value_size);
            Status ws = db->Put(wo, Slice(key), Slice(value));
            if (!ws.ok()) std::cerr << "Put failed: " << ws.ToString() << "\n";
        }
        total_writes += static_cast<uint64_t>(num_entries);
    }
    WaitForCompactionDrain(db, disable_auto_compaction);
    auto w1 = std::chrono::steady_clock::now();

    std::mt19937 rng(2026);
    ReadOptions ro;
    uint64_t found = 0;
    auto r0 = std::chrono::steady_clock::now();
    for (int i = 0; i < num_entries; ++i) {
        int k = rng() % num_entries;
        std::string key = FixedKey(k);
        std::string value;
        Status rs = db->Get(ro, Slice(key), &value);
        if (rs.ok()) ++found;
    }
    auto r1 = std::chrono::steady_clock::now();

    Result result;
    if (DBImpl* impl = dynamic_cast<DBImpl*>(db)) {
        impl->GetLevelFileStats(&result.level_file_counts, &result.level_total_bytes);
    }
    delete db;
    DirStats st = ScanDir(db_path);
    result.write_seconds = std::chrono::duration<double>(w1 - w0).count();
    result.read_seconds = std::chrono::duration<double>(r1 - r0).count();
    result.found = found;
    result.total_writes = total_writes;
    result.stats = st;
    return result;
}

std::string FormatLevelStats(const std::vector<uint64_t>& counts, const std::vector<uint64_t>& bytes) {
    if (counts.empty() || bytes.empty() || counts.size() != bytes.size()) return "n/a";
    std::string out;
    for (size_t i = 0; i < counts.size(); ++i) {
        if (!out.empty()) out.append("|");
        out.append("L").append(std::to_string(i))
            .append(":")
            .append(std::to_string(counts[i]))
            .append("f/")
            .append(std::to_string(bytes[i]))
            .append("b");
    }
    return out;
}

void PrintResult(const std::string& name, const Result& r, int num_entries) {
    double write_ops = r.total_writes / r.write_seconds;
    double read_ops = num_entries / r.read_seconds;
    std::cout << name
              << " write_s=" << r.write_seconds
              << " write_ops=" << write_ops
              << " total_writes=" << r.total_writes
              << " read_s=" << r.read_seconds
              << " read_ops=" << read_ops
              << " found=" << r.found
              << " sst_files=" << r.stats.sst_files
              << " log_files=" << r.stats.log_files
              << " total_bytes=" << r.stats.total_bytes
              << " level_stats=" << FormatLevelStats(r.level_file_counts, r.level_total_bytes)
              << "\n";
}

}  // namespace

int main(int argc, char** argv) {
    Config cfg;
    if (!ParseArgs(argc, argv, &cfg)) {
        std::cerr << "Usage: " << argv[0]
                  << " --num_entries=N --value_size=N --churn_rounds=N"
                  << " --hot_key_space=N --base_dir=PATH\n";
        return 1;
    }

    const std::string on_path = cfg.base_dir + "_on";
    const std::string off_path = cfg.base_dir + "_off";
    Result on = RunCase(on_path, false, cfg.num_entries, cfg.value_size, cfg.churn_rounds, cfg.hot_key_space);
    Result off = RunCase(off_path, true, cfg.num_entries, cfg.value_size, cfg.churn_rounds, cfg.hot_key_space);

    std::cout << "=== Compaction A/B Benchmark ===\n";
    std::cout << "num_entries=" << cfg.num_entries
              << ", value_size=" << cfg.value_size
              << ", churn_rounds=" << cfg.churn_rounds
              << ", hot_key_space=" << cfg.hot_key_space << "\n";
    PrintResult("compaction_on", on, cfg.num_entries);
    PrintResult("compaction_off", off, cfg.num_entries);
    return 0;
}

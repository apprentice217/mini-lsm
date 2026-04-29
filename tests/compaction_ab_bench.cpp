#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <random>
#include <string>
#include "db.h"
#include "options.h"

using namespace minidb;

namespace {

struct Config {
    int num_entries = 20000;
    int value_size = 100;
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
        if (ParseStringArg(arg, "--base_dir=", &cfg->base_dir)) continue;
        std::cerr << "Unknown arg: " << arg << "\n";
        return false;
    }
    return cfg->num_entries > 0 && cfg->value_size > 0;
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
    DirStats stats;
};

Result RunCase(const std::string& db_path, bool disable_auto_compaction, int num_entries, int value_size) {
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

    delete db;
    DirStats st = ScanDir(db_path);
    Result result;
    result.write_seconds = std::chrono::duration<double>(w1 - w0).count();
    result.read_seconds = std::chrono::duration<double>(r1 - r0).count();
    result.found = found;
    result.stats = st;
    return result;
}

void PrintResult(const std::string& name, const Result& r, int num_entries) {
    double write_ops = num_entries / r.write_seconds;
    double read_ops = num_entries / r.read_seconds;
    std::cout << name
              << " write_s=" << r.write_seconds
              << " write_ops=" << write_ops
              << " read_s=" << r.read_seconds
              << " read_ops=" << read_ops
              << " found=" << r.found
              << " sst_files=" << r.stats.sst_files
              << " log_files=" << r.stats.log_files
              << " total_bytes=" << r.stats.total_bytes
              << "\n";
}

}  // namespace

int main(int argc, char** argv) {
    Config cfg;
    if (!ParseArgs(argc, argv, &cfg)) {
        std::cerr << "Usage: " << argv[0]
                  << " --num_entries=N --value_size=N --base_dir=PATH\n";
        return 1;
    }

    const std::string on_path = cfg.base_dir + "_on";
    const std::string off_path = cfg.base_dir + "_off";
    Result on = RunCase(on_path, false, cfg.num_entries, cfg.value_size);
    Result off = RunCase(off_path, true, cfg.num_entries, cfg.value_size);

    std::cout << "=== Compaction A/B Benchmark ===\n";
    std::cout << "num_entries=" << cfg.num_entries << ", value_size=" << cfg.value_size << "\n";
    PrintResult("compaction_on", on, cfg.num_entries);
    PrintResult("compaction_off", off, cfg.num_entries);
    return 0;
}

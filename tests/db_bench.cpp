#include <cassert>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <sys/utsname.h>
#include "db.h"
#include "filter_policy.h"
#include "options.h"
#include "write_batch.h"

using namespace minidb;

class RandomGenerator {
public:
    RandomGenerator() : gen_(42) {
        for (int i = 0; i < 1048576; ++i) {
            data_.push_back(static_cast<char>(' ' + (gen_() % 95)));
        }
    }

    Slice Generate(size_t len) {
        if (pos_ + len > data_.size()) {
            pos_ = 0;
            assert(len < data_.size());
        }
        pos_ += len;
        return Slice(data_.data() + pos_ - len, len);
    }

private:
    std::mt19937 gen_;
    std::string data_;
    size_t pos_ = 0;
};

class Timer {
public:
    Timer() { Start(); }
    void Start() { start_ = std::chrono::steady_clock::now(); }
    void Stop() { end_ = std::chrono::steady_clock::now(); }

    double GetSeconds() const {
        std::chrono::duration<double> diff = end_ - start_;
        return diff.count();
    }

private:
    std::chrono::time_point<std::chrono::steady_clock> start_, end_;
};

struct BenchConfig {
    int num_entries = 100000;
    int key_size = 16;
    int value_size = 100;
    int batch_size = 1000;
    bool enable_bloom = true;
    bool sync_write = false;
    std::string db_name = "./bench_test_db";
    std::string output_csv = "./benchmark_results.csv";
};

void PrintUsage(const char* program) {
    std::cout
        << "Usage: " << program << " [options]\n"
        << "  --num_entries=N      Number of operations (default: 100000)\n"
        << "  --key_size=N         Key size in bytes (default: 16)\n"
        << "  --value_size=N       Value size in bytes (default: 100)\n"
        << "  --batch_size=N       Batch size for writes (default: 1000)\n"
        << "  --db_name=PATH       DB directory (default: ./bench_test_db)\n"
        << "  --output_csv=PATH    CSV output file (default: ./benchmark_results.csv)\n"
        << "  --sync_write=0|1     WAL sync on every write (default: 0)\n"
        << "  --enable_bloom=0|1   Enable bloom filter (default: 1)\n";
}

bool StartsWith(const std::string& text, const std::string& prefix) {
    return text.rfind(prefix, 0) == 0;
}

bool ParseIntArg(const std::string& arg, const std::string& key, int* out) {
    if (!StartsWith(arg, key)) return false;
    *out = std::stoi(arg.substr(key.size()));
    return true;
}

bool ParseBoolArg(const std::string& arg, const std::string& key, bool* out) {
    if (!StartsWith(arg, key)) return false;
    const std::string value = arg.substr(key.size());
    *out = (value == "1" || value == "true" || value == "TRUE");
    return true;
}

bool ParseStringArg(const std::string& arg, const std::string& key, std::string* out) {
    if (!StartsWith(arg, key)) return false;
    *out = arg.substr(key.size());
    return true;
}

bool ParseArgs(int argc, char** argv, BenchConfig* cfg) {
    for (int i = 1; i < argc; ++i) {
        std::string arg(argv[i]);
        if (arg == "--help" || arg == "-h") {
            PrintUsage(argv[0]);
            return false;
        }
        if (ParseIntArg(arg, "--num_entries=", &cfg->num_entries)) continue;
        if (ParseIntArg(arg, "--key_size=", &cfg->key_size)) continue;
        if (ParseIntArg(arg, "--value_size=", &cfg->value_size)) continue;
        if (ParseIntArg(arg, "--batch_size=", &cfg->batch_size)) continue;
        if (ParseBoolArg(arg, "--sync_write=", &cfg->sync_write)) continue;
        if (ParseBoolArg(arg, "--enable_bloom=", &cfg->enable_bloom)) continue;
        if (ParseStringArg(arg, "--db_name=", &cfg->db_name)) continue;
        if (ParseStringArg(arg, "--output_csv=", &cfg->output_csv)) continue;
        std::cerr << "Unknown argument: " << arg << "\n";
        PrintUsage(argv[0]);
        return false;
    }
    if (cfg->num_entries <= 0 || cfg->key_size <= 0 || cfg->value_size <= 0 || cfg->batch_size <= 0) {
        std::cerr << "All numeric options must be > 0\n";
        return false;
    }
    return true;
}

std::string ReadCpuModel() {
    std::ifstream ifs("/proc/cpuinfo");
    std::string line;
    while (std::getline(ifs, line)) {
        if (StartsWith(line, "model name")) {
            size_t pos = line.find(':');
            if (pos != std::string::npos) return line.substr(pos + 2);
        }
    }
    return "unknown";
}

int ReadCpuThreads() {
    std::ifstream ifs("/proc/cpuinfo");
    std::string line;
    int count = 0;
    while (std::getline(ifs, line)) {
        if (StartsWith(line, "processor")) ++count;
    }
    return count;
}

std::string ReadMemTotal() {
    std::ifstream ifs("/proc/meminfo");
    std::string line;
    while (std::getline(ifs, line)) {
        if (StartsWith(line, "MemTotal:")) return line;
    }
    return "MemTotal: unknown";
}

std::string ReadKernelVersion() {
    struct utsname info;
    if (uname(&info) == 0) {
        return std::string(info.sysname) + " " + info.release;
    }
    return "unknown";
}

void PrintEnvironment() {
    std::cout << "=== Environment ===" << std::endl;
    std::cout << "kernel: " << ReadKernelVersion() << std::endl;
    std::cout << "cpu_model: " << ReadCpuModel() << std::endl;
    std::cout << "cpu_threads: " << ReadCpuThreads() << std::endl;
    std::cout << "memory: " << ReadMemTotal() << std::endl;
#ifdef MINIDB_BUILD_TYPE
    std::cout << "build_type: " << MINIDB_BUILD_TYPE << std::endl;
#else
    std::cout << "build_type: unknown" << std::endl;
#endif
#ifdef MINIDB_GIT_HASH
    std::cout << "git_hash: " << MINIDB_GIT_HASH << std::endl;
#else
    std::cout << "git_hash: unknown" << std::endl;
#endif
    std::cout << "===================" << std::endl;
}

void PrintStats(const std::string& name, int count, double seconds, size_t bytes) {
    double ops = count / seconds;
    double throughput_mb = (bytes / 1048576.0) / seconds;
    double micros_per_op = (seconds * 1000000.0) / count;

    std::cout << std::left << std::setw(20) << name
              << " : " << std::fixed << std::setprecision(1) << micros_per_op << " micros/op; "
              << std::fixed << std::setprecision(1) << ops << " ops/sec; "
              << std::fixed << std::setprecision(1) << throughput_mb << " MB/s"
              << std::endl;
}

void AppendCsvIfNeeded(const BenchConfig& cfg, const std::string& name, int count, double seconds, size_t bytes) {
    const bool file_exists = static_cast<bool>(std::ifstream(cfg.output_csv));
    std::ofstream ofs(cfg.output_csv, std::ios::app);
    if (!file_exists) {
        ofs << "name,num_entries,key_size,value_size,batch_size,sync_write,enable_bloom,seconds,ops_per_sec,mb_per_sec,micros_per_op\n";
    }
    double ops = count / seconds;
    double throughput_mb = (bytes / 1048576.0) / seconds;
    double micros_per_op = (seconds * 1000000.0) / count;
    ofs << name << ","
        << cfg.num_entries << ","
        << cfg.key_size << ","
        << cfg.value_size << ","
        << cfg.batch_size << ","
        << (cfg.sync_write ? 1 : 0) << ","
        << (cfg.enable_bloom ? 1 : 0) << ","
        << std::fixed << std::setprecision(6) << seconds << ","
        << ops << ","
        << throughput_mb << ","
        << micros_per_op << "\n";
}

void RunWriteBenchmark(DB* db, const BenchConfig& cfg, const std::string& phase, bool sequential, RandomGenerator* gen) {
    const int num_entries = cfg.num_entries;
    const size_t bytes_per_op = static_cast<size_t>(cfg.key_size) + static_cast<size_t>(cfg.value_size);
    const size_t total_bytes = static_cast<size_t>(num_entries) * bytes_per_op;
    WriteOptions write_opt;
    write_opt.sync = cfg.sync_write;
    WriteBatch batch;
    Timer timer;
    timer.Start();
    for (int i = 0; i < num_entries; i++) {
        char key_buf[64];
        int key_num = sequential ? i : (rand() % num_entries);
        std::snprintf(key_buf, sizeof(key_buf), "%016d", key_num);
        batch.Put(Slice(key_buf, cfg.key_size), gen->Generate(cfg.value_size));
        if ((i + 1) % cfg.batch_size == 0) {
            Status s = db->Write(write_opt, &batch);
            assert(s.ok());
            batch.Clear();
        }
    }
    if (batch.ApproximateSize() > 0) {
        Status s = db->Write(write_opt, &batch);
        assert(s.ok());
    }
    timer.Stop();
    PrintStats(phase, num_entries, timer.GetSeconds(), total_bytes);
    AppendCsvIfNeeded(cfg, phase, num_entries, timer.GetSeconds(), total_bytes);
}

void RunReadRandomBenchmark(DB* db, const BenchConfig& cfg) {
    const int num_entries = cfg.num_entries;
    const size_t bytes_per_op = static_cast<size_t>(cfg.key_size) + static_cast<size_t>(cfg.value_size);
    const size_t total_bytes = static_cast<size_t>(num_entries) * bytes_per_op;
    ReadOptions read_opt;
    std::mt19937 rng(2026);
    int found = 0;
    Timer timer;
    timer.Start();
    for (int i = 0; i < num_entries; i++) {
        char key_buf[64];
        int random_key = static_cast<int>(rng() % static_cast<uint32_t>(num_entries));
        std::snprintf(key_buf, sizeof(key_buf), "%016d", random_key);
        std::string value;
        Status s = db->Get(read_opt, Slice(key_buf, cfg.key_size), &value);
        if (s.ok()) found++;
    }
    timer.Stop();
    PrintStats("readrandom", num_entries, timer.GetSeconds(), total_bytes);
    std::cout << "  (Found " << found << " out of " << num_entries << " records)" << std::endl;
    AppendCsvIfNeeded(cfg, "readrandom", num_entries, timer.GetSeconds(), total_bytes);
}

int main(int argc, char** argv) {
    std::cout << "=== Mini-LevelDB Benchmark ===" << std::endl;
    BenchConfig cfg;
    if (!ParseArgs(argc, argv, &cfg)) return 1;

    PrintEnvironment();
    std::cout << "num_entries=" << cfg.num_entries
              << ", key_size=" << cfg.key_size
              << ", value_size=" << cfg.value_size
              << ", batch_size=" << cfg.batch_size
              << ", sync_write=" << (cfg.sync_write ? 1 : 0)
              << ", enable_bloom=" << (cfg.enable_bloom ? 1 : 0) << std::endl;

    const minidb::FilterPolicy* bloom = cfg.enable_bloom ? minidb::NewBloomFilterPolicy(10) : nullptr;
    Options options;
    options.create_if_missing = true;
    options.filter_policy = bloom;

    DB* db = nullptr;
    Status s = DB::Open(options, cfg.db_name, &db);
    assert(s.ok());

    RandomGenerator gen;
    std::cout << "Starting FillSeq..." << std::endl;
    RunWriteBenchmark(db, cfg, "fillseq", true, &gen);
    RunReadRandomBenchmark(db, cfg);
    std::cout << "Starting FillRandom..." << std::endl;
    RunWriteBenchmark(db, cfg, "fillrandom", false, &gen);

    delete db;
    if (bloom != nullptr) delete bloom;
    std::cout << "=== Benchmark Completed ===" << std::endl;
    return 0;
}
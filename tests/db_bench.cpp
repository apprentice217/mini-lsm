#include <iostream>
#include <chrono>
#include <random>
#include <string>
#include <vector>
#include <iomanip>
#include <cstdio>
#include <cassert>
#include "db.h"
#include "options.h"
#include "write_batch.h"

using namespace minidb;

// 极速伪随机字符串生成器 (榨干 CPU，避免随机数生成成为测试瓶颈)
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

// RAII 风格的高精度计时器
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

// 格式化输出压测结果
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

int main() {
    std::cout << "=== Mini-LevelDB Benchmark ===" << std::endl;
    
    // 压测参数配置：10万条数据，Key 16字节，Value 100字节 (经典配置)
    const int num_entries = 100000;
    const int key_size = 16;
    const int value_size = 100;
    const size_t bytes_per_op = key_size + value_size;
    const size_t total_bytes = num_entries * bytes_per_op;

    Options options;
    options.create_if_missing = true;
    std::string db_name = "./bench_test_db";

    DB* db = nullptr;
    Status s = DB::Open(options, db_name, &db);
    assert(s.ok());

    RandomGenerator gen;
    Timer timer;
    WriteOptions write_opt;
    ReadOptions read_opt;

    // ---------------------------------------------------------
    // 阶段 1：顺序写测试 (FillSeq)
    // 考验 MemTable 的极速尾部追加与底层写 WAL 性能
    // ---------------------------------------------------------
    std::cout << "Starting FillSeq..." << std::endl;
    timer.Start();
    // write_opt.sync = false; 

    minidb::WriteBatch batch;
    for (int i = 0; i < num_entries; i++) {
        char key_buf[32];
        std::snprintf(key_buf, sizeof(key_buf), "%016d", i); // 生成 0000000000000000 递增 Key
        // 把请求暂存到 Batch 中，先不惊动底层引擎
        batch.Put(minidb::Slice(key_buf, 16), gen.Generate(value_size));
        // 【核心提速】：满 1000 条，合并提交一次！
        // 这把 1000 次可能发生的系统调用和锁竞争，压缩成了 1 次！
        if ((i + 1) % 1000 == 0) {
            Status s = db->Write(write_opt, &batch);
            assert(s.ok());
            batch.Clear(); // 提交完记得清空弹夹
        }
    }
    // 把最后不满 1000 条的尾巴数据提交掉
    if (batch.ApproximateSize() > 0) {
        Status s = db->Write(write_opt, &batch);
        assert(s.ok());
    }
    timer.Stop();
    PrintStats("fillseq", num_entries, timer.GetSeconds(), total_bytes);

    // ---------------------------------------------------------
    // 阶段 2：随机读测试 (ReadRandom)
    // 考验 SkipList 的寻址能力与 SSTable 的磁盘点查能力
    // ---------------------------------------------------------
    std::mt19937 rng(2026);
    int found = 0;
    timer.Start();
    for (int i = 0; i < num_entries; i++) {
        char key_buf[32];
        int random_key = rng() % num_entries;
        std::snprintf(key_buf, sizeof(key_buf), "%016d", random_key);
        std::string value;
        Status s = db->Get(read_opt, Slice(key_buf, key_size), &value);
        if (s.ok()) found++;
    }
    timer.Stop();
    PrintStats("readrandom", num_entries, timer.GetSeconds(), total_bytes);
    std::cout << "  (Found " << found << " out of " << num_entries << " records)" << std::endl;

    // ---------------------------------------------------------
    // 阶段 3：随机写测试 (FillRandom)
    // 考验 SkipList 的动态插入平衡能力以及 Compaction 机制
    // ---------------------------------------------------------
    std::cout << "Starting FillRandom..." << std::endl;

    timer.Start();

    minidb::WriteBatch rand_batch;
    for (int i = 0; i < num_entries; i++) {
        char key_buf[32];
        std::snprintf(key_buf, sizeof(key_buf), "%016d", rand() % num_entries);

        rand_batch.Put(minidb::Slice(key_buf, 16), gen.Generate(value_size));

        // 同样，满 1000 条合并提交
        if ((i + 1) % 1000 == 0) {
            Status s = db->Write(write_opt, &rand_batch);
            assert(s.ok());
            rand_batch.Clear();
        }
    }

    if (rand_batch.ApproximateSize() > 0) {
        Status s = db->Write(write_opt, &rand_batch);
        assert(s.ok());
    }

    timer.Stop();
    PrintStats("fillrandom", num_entries, timer.GetSeconds(), total_bytes);

    delete db;
    std::cout << "=== Benchmark Completed ===" << std::endl;

    return 0;
}
#include <chrono>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include "db.h"
#include "options.h"
#include "write_batch.h"

using namespace minidb;

namespace {

void Check(bool condition, const std::string& msg) {
    if (!condition) {
        throw std::runtime_error(msg);
    }
}

void CleanDir(const std::string& path) {
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
}

std::string UniqueTestDir(const std::string& prefix) {
    const auto now = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    return "./tmp_" + prefix + "_" + std::to_string(now);
}

std::string FixedKey(const std::string& key) {
    if (key.size() >= 16) return key.substr(0, 16);
    return key + std::string(16 - key.size(), '_');
}

DB* OpenDb(const std::string& path, const Options& options) {
    DB* db = nullptr;
    Status s = DB::Open(options, path, &db);
    Check(s.ok(), "DB::Open failed: " + s.ToString());
    return db;
}

void AssertGetOk(DB* db, const std::string& key, const std::string& expected) {
    std::string value;
    const std::string fixed = FixedKey(key);
    Status s = db->Get(ReadOptions(), Slice(fixed), &value);
    Check(s.ok(), "Get failed for key: " + key + ", status=" + s.ToString());
    Check(value == expected, "Value mismatch for key: " + key);
}

void TestBatchAtomicWrite() {
    const std::string path = UniqueTestDir("batch");
    CleanDir(path);
    Options options;
    options.create_if_missing = true;

    DB* db = OpenDb(path, options);
    WriteBatch batch;
    const std::string u1 = FixedKey("u1");
    const std::string u2 = FixedKey("u2");
    batch.Put(Slice(u1), Slice("v1"));
    batch.Put(Slice(u2), Slice("v2"));
    batch.Put(Slice(u2), Slice("v3"));
    Check(db->Write(WriteOptions(), &batch).ok(), "WriteBatch apply failed");

    AssertGetOk(db, "u1", "v1");
    AssertGetOk(db, "u2", "v3");
    delete db;
    CleanDir(path);
}

void TestOverwriteDeleteSemantics() {
    const std::string path = UniqueTestDir("semantics");
    CleanDir(path);
    Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 8 * 1024;

    DB* db = OpenDb(path, options);
    WriteOptions wo;

    const std::string a = FixedKey("a");
    Check(db->Put(wo, Slice(a), Slice("1")).ok(), "Put a=1 failed");
    Check(db->Put(wo, Slice(a), Slice("2")).ok(), "Put a=2 failed");

    for (int i = 0; i < 40; ++i) {
        std::string key = FixedKey("hot_" + std::to_string(i % 4));
        std::string value = "value_" + std::to_string(i);
        Check(db->Put(wo, Slice(key), Slice(value)).ok(), "Put hot key failed");
    }

    AssertGetOk(db, "a", "2");
    delete db;

    db = OpenDb(path, options);
    AssertGetOk(db, "a", "2");
    delete db;
    CleanDir(path);
}

}  // namespace

int main() {
    std::cout << "[db_correctness] running batch atomic write test..." << std::endl;
    TestBatchAtomicWrite();
    std::cout << "[db_correctness] running overwrite/delete semantics test..." << std::endl;
    TestOverwriteDeleteSemantics();
    std::cout << "[db_correctness] all tests passed" << std::endl;
    return 0;
}

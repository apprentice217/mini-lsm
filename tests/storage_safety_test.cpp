#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <cerrno>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include "db_impl.h"
#include "version_set.h"
#include "table_builder.h"
#include "coding.h"

using namespace minidb;
namespace fs = std::filesystem;
namespace {
struct Event { std::string kind, path; };
std::mutex event_mutex;
std::vector<Event> events;
std::string fail_dir;
int fail_countdown = 0;
std::atomic<bool> injected{false};
void Check(bool ok, const std::string& message) {
    if (!ok) throw std::runtime_error(message);
}
void Ok(const Status& s) { Check(s.ok(), s.ToString()); }
struct TempDir {
    std::string path;
    TempDir() {
        char pattern[] = "/tmp/mini-lsm-safety-XXXXXX";
        char* p = mkdtemp(pattern);
        Check(p != nullptr, "mkdtemp");
        path = p;
    }
    ~TempDir() { fs::remove_all(path); }
};
void Record(const std::string& kind, const std::string& path) {
    std::lock_guard<std::mutex> lock(event_mutex);
    events.push_back({kind, path});
}
void Reset(const std::string& dir = "", int count = 0) {
    std::lock_guard<std::mutex> lock(event_mutex);
    events.clear(); fail_dir = dir; fail_countdown = count; injected = false;
}
std::vector<Event> Events() {
    std::lock_guard<std::mutex> lock(event_mutex);
    return events;
}
std::unique_ptr<DB> Open(const std::string& path, Options options = Options()) {
    options.create_if_missing = true;
    DB* raw = nullptr;
    Ok(DB::Open(options, path, &raw));
    return std::unique_ptr<DB>(raw);
}
std::string IKey(const std::string& key, uint64_t seq = 1) {
    std::string result = key;
    PutFixed64(&result, (seq << 8) | kTypeValue);
    return result;
}
template<class F> void Wait(F predicate) {
    const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!predicate()) {
        Check(std::chrono::steady_clock::now() < end, "background work timed out");
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
}
void TestLocks() {
    TempDir dir;
    auto db = Open(dir.path);
    const auto before = fs::file_size(dir.path + "/CURRENT");
    DB* second = nullptr;
    Status s = DB::Open(Options(), dir.path, &second);
    Check(!s.ok() && second == nullptr, "same-process duplicate open succeeded");
    pid_t child = fork();
    Check(child >= 0, "fork");
    if (child == 0) {
        execl("/proc/self/exe", "storage_safety_test", "--locked", dir.path.c_str(), nullptr);
        _exit(127);
    }
    int status = 0;
    Check(waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0,
          "cross-process duplicate open succeeded");
    Check(fs::file_size(dir.path + "/CURRENT") == before, "duplicate open modified CURRENT");
    db.reset();
    Check(fs::exists(dir.path + "/LOCK"), "LOCK must not be unlinked");
    db = Open(dir.path);
}
void TestLiveVersions() {
    TempDir dir;
    Options options;
    InternalKeyComparator cmp(options.comparator);
    options.comparator = &cmp;
    VersionSet versions(dir.path, &options, nullptr, &cmp);
    const auto number = versions.NextFileNumber();
    const std::string key = IKey("old-version-key");
    VersionEdit add;
    add.AddFile(0, number, 100, key, key);
    Ok(versions.LogAndApply(&add));
    auto old = versions.current();
    VersionEdit remove;
    remove.DeleteFile(0, number);
    Ok(versions.LogAndApply(&remove));
    std::set<uint64_t> live;
    versions.AddLiveFiles(&live);
    Check(live.count(number) == 1, "reader's old version lost its SST");
    // 多次版本切换也不能丢掉仍存活的旧版本。
    for (int i = 0; i < 3; ++i) { VersionEdit edit; Ok(versions.LogAndApply(&edit)); }
    live.clear(); versions.AddLiveFiles(&live);
    Check(live.count(number) == 1, "old version lost after repeated publication");
    old.reset(); live.clear(); versions.AddLiveFiles(&live);
    Check(live.count(number) == 0, "unreferenced version retained forever");
}
void TestIteratorKeepsFiles() {
    TempDir dir;
    Options options;
    options.write_buffer_size = 32768;
    options.l0_compaction_trigger = 4;
    InternalKeyComparator cmp(options.comparator);
    Options internal = options; internal.comparator = &cmp;
    std::vector<std::string> originals;
    {
        VersionSet versions(dir.path, &internal, nullptr, &cmp);
        VersionEdit edit;
        for (int i = 0; i < 3; ++i) {
            uint64_t number = versions.NextFileNumber();
            char name[64]; std::snprintf(name, sizeof(name), "/%06llu.sst", (unsigned long long)number);
            originals.push_back(dir.path + name);
            WritableFile* raw = nullptr;
            Ok(NewWritableFile(originals.back(), &raw));
            std::unique_ptr<WritableFile> file(raw);
            TableBuilder builder(internal, file.get());
            std::string key = IKey("seed-key-00000001");
            builder.Add(key, "seed"); Ok(builder.Finish()); Ok(file->Sync());
            edit.AddFile(0, number, builder.FileSize(), key, key);
        }
        Ok(SyncDir(dir.path)); versions.set_last_sequence(1);
        Ok(versions.LogAndApply(&edit));
    }
    auto db = Open(dir.path, options);
    std::unique_ptr<Iterator> iterator(db->NewIterator(ReadOptions()));
    Ok(db->Put(WriteOptions(), "write-key-0000001", std::string(65536, 'x')));
    Ok(db->Put(WriteOptions(), "write-key-0000002", "v"));
    auto* impl = static_cast<DBImpl*>(db.get());
    Wait([&] {
        std::vector<uint64_t> files, bytes;
        impl->GetLevelFileStats(&files, &bytes);
        return files[1] != 0 && files[0] < 4;
    });
    for (const auto& file : originals) Check(fs::exists(file), "iterator's old SST was deleted");
    iterator.reset();
    Ok(db->Put(WriteOptions(), "write-key-0000003", std::string(65536, 'y')));
    Ok(db->Put(WriteOptions(), "write-key-0000004", "v"));
    Wait([&] { for (const auto& file : originals) if (fs::exists(file)) return false; return true; });
    // 验证 flush 和 compaction 输出都先同步 SST 和目录，再提交 MANIFEST。
    const auto recorded = Events();
    int outputs = 0;
    for (size_t i = 0; i < recorded.size(); ++i) {
        if (recorded[i].kind != "file" || fs::path(recorded[i].path).extension() != ".sst") continue;
        if (recorded[i].path.find(dir.path) != 0) continue;
        ++outputs;
        bool synced_dir = false;
        for (size_t j = i + 1; j < recorded.size(); ++j) {
            if (recorded[j].kind == "dir" && recorded[j].path == dir.path) synced_dir = true;
            if (recorded[j].kind == "file" && recorded[j].path.find(dir.path + "/MANIFEST-") == 0) {
                Check(synced_dir, "MANIFEST synced before SST directory"); break;
            }
        }
    }
    Check(outputs >= 6, "flush/compaction SST sync paths were not exercised");
}
void TestPublicationOrderAndFailure() {
    TempDir dir;
    Reset();
    auto db = Open(dir.path);
    auto recorded = Events();
    std::vector<std::string> sequence;
    for (const auto& e : recorded) {
        if (e.path == dir.path && e.kind == "dir") sequence.push_back("dir");
        else if (e.path.find(dir.path + "/") == 0) {
            if (e.kind == "rename") sequence.push_back("rename");
            else if (fs::path(e.path).extension() == ".log") sequence.push_back("wal");
            else if (e.path.find("/MANIFEST-") != std::string::npos) sequence.push_back("manifest");
            else if (fs::path(e.path).filename() == "CURRENT.tmp") sequence.push_back("current");
        }
    }
    Check(sequence == std::vector<std::string>({"wal", "dir", "manifest", "dir", "current", "rename", "dir"}),
          "incorrect initial publication durability order");
    WriteOptions write; write.sync = true;
    Ok(db->Put(write, "recovery-key-001", "persisted"));
    db.reset();
    // 分别模拟 WAL、MANIFEST 目录项同步和 CURRENT rename 后目录同步失败。
    for (int stage = 1; stage <= 3; ++stage) {
        Reset(dir.path, stage);
        DB* raw = nullptr;
        Status s = DB::Open(Options(), dir.path, &raw);
        Check(!s.ok() && raw == nullptr && injected, "directory sync error was not propagated");
        Reset();
        db = Open(dir.path); // 打开失败必须释放锁；重试应能恢复已有数据。
        std::string value;
        Ok(db->Get(ReadOptions(), "recovery-key-001", &value));
        Check(value == "persisted", "failed publication lost old data");
        db.reset();
    }
    Check(!SyncDir(dir.path + "/missing").ok(), "SyncDir accepted missing directory");
}
void TestWalRotationFailure() {
    TempDir dir;
    Options options; options.write_buffer_size = 32768; options.disable_auto_compaction = true;
    auto db = Open(dir.path, options);
    WriteOptions write; write.sync = true;
    Ok(db->Put(write, "rotation-key-001", std::string(65536, 'z')));
    Reset(dir.path, 1);
    Check(!db->Put(write, "rotation-key-002", "value").ok() && injected,
          "WAL rotation ignored directory sync failure");
    Reset();
    Ok(db->Put(write, "rotation-key-002", "value"));
    db.reset();
    db = Open(dir.path, options);
    std::string value;
    Ok(db->Get(ReadOptions(), "rotation-key-002", &value));
    Check(value == "value", "rotation retry lost write");
}
void TestSstPublicationFailure() {
    TempDir dir;
    Options options; options.write_buffer_size = 32768; options.disable_auto_compaction = true;
    auto db = Open(dir.path, options);
    WriteOptions write; write.sync = true;
    Ok(db->Put(write, "sst-failure-0001", std::string(65536, 's')));
    // 第一次目录同步是新 WAL，第二次是后台 flush 产生的 SST。
    Reset(dir.path, 2);
    Ok(db->Put(write, "sst-failure-0002", "second"));
    Wait([] { return injected.load(); });
    Wait([&] { return !db->Put(write, "sst-failure-0003", "probe").ok(); });
    std::vector<uint64_t> files, bytes;
    static_cast<DBImpl*>(db.get())->GetLevelFileStats(&files, &bytes);
    Check(files[0] == 0, "failed SST directory sync still published metadata");
    db.reset(); Reset();
    db = Open(dir.path, options);
    std::string value;
    Ok(db->Get(ReadOptions(), "sst-failure-0001", &value));
    Check(value == std::string(65536, 's'), "failed SST publication discarded recovery WAL");
}
} // namespace

// GNU ld syscall wrappers allow deterministic failure injection without production hooks.
extern "C" int __real_fsync(int);
extern "C" int __real_rename(const char*, const char*);
extern "C" int __wrap_fsync(int fd) {
    char link[64], path[4096];
    std::snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    ssize_t len = readlink(link, path, sizeof(path) - 1);
    std::string filename = len >= 0 ? std::string(path, static_cast<size_t>(len)) : "";
    struct stat st;
    bool directory = fstat(fd, &st) == 0 && S_ISDIR(st.st_mode);
    {
        std::lock_guard<std::mutex> lock(event_mutex);
        if (directory && filename == fail_dir && fail_countdown > 0 && --fail_countdown == 0) {
            injected = true; errno = EIO; return -1;
        }
    }
    int rc = __real_fsync(fd);
    if (rc == 0) Record(directory ? "dir" : "file", filename);
    return rc;
}
extern "C" int __wrap_rename(const char* from, const char* to) {
    int rc = __real_rename(from, to);
    if (rc == 0) Record("rename", to);
    return rc;
}
int main(int argc, char** argv) {
    if (argc == 3 && std::string(argv[1]) == "--locked") {
        DB* db = nullptr;
        Status s = DB::Open(Options(), argv[2], &db);
        delete db;
        return !s.ok() && db == nullptr ? 0 : 1;
    }
    try {
        std::cerr << "[test] locks\n"; TestLocks();
        std::cerr << "[test] live versions\n"; TestLiveVersions();
        std::cerr << "[test] iterator files\n"; Reset(); TestIteratorKeepsFiles();
        std::cerr << "[test] publication\n"; TestPublicationOrderAndFailure();
        std::cerr << "[test] WAL rotation\n"; TestWalRotationFailure();
        std::cerr << "[test] SST failure\n"; TestSstPublicationFailure();
        std::cout << "storage safety tests passed\n";
    } catch (const std::exception& e) {
        std::cerr << "FAIL: " << e.what() << '\n'; return 1;
    }
}

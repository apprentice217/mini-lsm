#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <map>
#include <random>
#include <string>
#include <vector>
#include "arena.h"
#include "skiplist.h"

namespace {

struct IntComparator {
    int operator()(uint64_t a, uint64_t b) const {
        if (a < b) return -1;
        if (a > b) return 1;
        return 0;
    }
};

struct Config {
    int n = 50000;
    int lookup = 50000;
    int seed = 42;
};

bool StartsWith(const std::string& text, const std::string& prefix) {
    return text.rfind(prefix, 0) == 0;
}

bool ParseIntArg(const std::string& arg, const std::string& key, int* out) {
    if (!StartsWith(arg, key)) return false;
    *out = std::stoi(arg.substr(key.size()));
    return true;
}

bool ParseArgs(int argc, char** argv, Config* cfg) {
    for (int i = 1; i < argc; ++i) {
        std::string arg(argv[i]);
        if (ParseIntArg(arg, "--n=", &cfg->n)) continue;
        if (ParseIntArg(arg, "--lookup=", &cfg->lookup)) continue;
        if (ParseIntArg(arg, "--seed=", &cfg->seed)) continue;
        std::cerr << "Unknown arg: " << arg << "\n";
        return false;
    }
    return cfg->n > 0 && cfg->lookup > 0;
}

template <typename F>
double TimeMillis(F&& fn) {
    auto t0 = std::chrono::steady_clock::now();
    fn();
    auto t1 = std::chrono::steady_clock::now();
    return std::chrono::duration<double, std::milli>(t1 - t0).count();
}

void PrintLine(const std::string& name, double insert_ms, double lookup_ms, double iter_ms) {
    std::cout << name
              << " insert_ms=" << insert_ms
              << " lookup_ms=" << lookup_ms
              << " iter_ms=" << iter_ms << "\n";
}

}  // namespace

int main(int argc, char** argv) {
    Config cfg;
    if (!ParseArgs(argc, argv, &cfg)) {
        std::cerr << "Usage: " << argv[0] << " --n=N --lookup=N [--seed=N]\n";
        return 1;
    }

    std::cout << "[memtable_ds_bench] start n=" << cfg.n
              << " lookup=" << cfg.lookup
              << " seed=" << cfg.seed << std::endl;

    std::vector<uint64_t> keys(static_cast<size_t>(cfg.n));
    for (int i = 0; i < cfg.n; ++i) keys[static_cast<size_t>(i)] = static_cast<uint64_t>(i + 1);
    std::mt19937_64 rng(static_cast<uint64_t>(cfg.seed));
    std::shuffle(keys.begin(), keys.end(), rng);

    std::vector<uint64_t> lookups(static_cast<size_t>(cfg.lookup));
    for (int i = 0; i < cfg.lookup; ++i) {
        lookups[static_cast<size_t>(i)] = keys[static_cast<size_t>(i % cfg.n)];
    }
    std::shuffle(lookups.begin(), lookups.end(), rng);

    // SkipList benchmark
    minidb::Arena arena;
    IntComparator cmp;
    minidb::SkipList<uint64_t, IntComparator> list(cmp, &arena);
    std::cout << "[memtable_ds_bench] phase=skiplist_insert" << std::endl;
    auto s0 = std::chrono::steady_clock::now();
    const int insert_step = std::max(1, cfg.n / 10);
    for (int i = 0; i < cfg.n; ++i) {
        list.Insert(keys[static_cast<size_t>(i)]);
        if ((i + 1) % insert_step == 0 || i + 1 == cfg.n) {
            std::cout << "  progress " << (i + 1) << "/" << cfg.n << std::endl;
        }
    }
    auto s1 = std::chrono::steady_clock::now();
    double skip_insert = std::chrono::duration<double, std::milli>(s1 - s0).count();

    size_t skip_hits = 0;
    std::cout << "[memtable_ds_bench] phase=skiplist_lookup" << std::endl;
    auto sl0 = std::chrono::steady_clock::now();
    const int lookup_step = std::max(1, cfg.lookup / 10);
    for (int i = 0; i < cfg.lookup; ++i) {
        if (list.Contains(lookups[static_cast<size_t>(i)])) ++skip_hits;
        if ((i + 1) % lookup_step == 0 || i + 1 == cfg.lookup) {
            std::cout << "  progress " << (i + 1) << "/" << cfg.lookup << std::endl;
        }
    }
    auto sl1 = std::chrono::steady_clock::now();
    double skip_lookup = std::chrono::duration<double, std::milli>(sl1 - sl0).count();

    uint64_t skip_iter_sum = 0;
    std::cout << "[memtable_ds_bench] phase=skiplist_iter" << std::endl;
    double skip_iter = TimeMillis([&]() {
        minidb::SkipList<uint64_t, IntComparator>::Iterator it(&list);
        for (it.SeekToFirst(); it.Valid(); it.Next()) skip_iter_sum += it.key();
    });

    // std::map (RB-Tree) benchmark
    std::map<uint64_t, uint64_t> tree;
    std::cout << "[memtable_ds_bench] phase=rbtree_insert" << std::endl;
    double tree_insert = TimeMillis([&]() {
        for (uint64_t k : keys) tree.emplace(k, k);
    });
    size_t tree_hits = 0;
    std::cout << "[memtable_ds_bench] phase=rbtree_lookup" << std::endl;
    double tree_lookup = TimeMillis([&]() {
        for (uint64_t k : lookups) {
            if (tree.find(k) != tree.end()) ++tree_hits;
        }
    });
    uint64_t tree_iter_sum = 0;
    std::cout << "[memtable_ds_bench] phase=rbtree_iter" << std::endl;
    double tree_iter = TimeMillis([&]() {
        for (const auto& kv : tree) tree_iter_sum += kv.first;
    });

    std::cout << "=== MemTable DS Benchmark ===\n";
    std::cout << "n=" << cfg.n << ", lookup=" << cfg.lookup << ", seed=" << cfg.seed << "\n";
    PrintLine("skiplist", skip_insert, skip_lookup, skip_iter);
    PrintLine("rbtree", tree_insert, tree_lookup, tree_iter);
    std::cout << "sanity hits: skiplist=" << skip_hits << ", rbtree=" << tree_hits << "\n";
    std::cout << "sanity iter_sum: skiplist=" << skip_iter_sum << ", rbtree=" << tree_iter_sum << "\n";
    return 0;
}

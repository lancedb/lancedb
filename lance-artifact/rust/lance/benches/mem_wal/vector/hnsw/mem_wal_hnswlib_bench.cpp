// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

#include "hnswlib/hnswlib.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

struct Args {
    size_t rows = 1000000;
    size_t dim = 1024;
    size_t queries = 1000;
    size_t truth_queries = 100;
    size_t k = 10;
    size_t m = 12;
    size_t ef_construction = 64;
    size_t ef_search = 64;
    size_t threads = std::thread::hardware_concurrency();
    uint64_t seed = 100;
    size_t clusters = 4096;
    float noise = 0.05f;
    size_t query_repeats = 1;
    double insert_seconds = 0.0;
    double query_seconds = 0.0;
};

Args parse_args(int argc, char **argv);
size_t parse_size(const std::string &value);
uint64_t parse_u64(const std::string &value);
using Clock = std::chrono::steady_clock;
Clock::time_point clock_now();
double elapsed_seconds(Clock::time_point start);
std::vector<float> generate_vectors(const Args &args);
std::vector<size_t> query_ids(const Args &args);
std::vector<size_t> query_ids(const Args &args, size_t count);
std::vector<size_t> exact_top_k(const float *query, const std::vector<float> &data, const Args &args);
float l2_distance(const float *left, const float *right, size_t dim);
float vector_value(size_t row, size_t col, const Args &args);
float unit_f32(uint64_t key);
uint64_t splitmix64(uint64_t x);

template <class Function>
void parallel_for(size_t start, size_t end, size_t num_threads, Function fn) {
    if (num_threads == 0) {
        num_threads = std::thread::hardware_concurrency();
    }
    if (num_threads <= 1) {
        for (size_t id = start; id < end; ++id) {
            fn(id, 0);
        }
        return;
    }

    std::vector<std::thread> threads;
    std::atomic<size_t> current(start);
    std::exception_ptr last_exception = nullptr;
    std::mutex exception_mutex;

    for (size_t thread_id = 0; thread_id < num_threads; ++thread_id) {
        threads.emplace_back([&, thread_id] {
            while (true) {
                size_t id = current.fetch_add(1);
                if (id >= end) {
                    break;
                }
                try {
                    fn(id, thread_id);
                } catch (...) {
                    std::unique_lock<std::mutex> lock(exception_mutex);
                    last_exception = std::current_exception();
                    current = end;
                    break;
                }
            }
        });
    }
    for (auto &thread : threads) {
        thread.join();
    }
    if (last_exception) {
        std::rethrow_exception(last_exception);
    }
}

int main(int argc, char **argv) {
    try {
        Args args = parse_args(argc, argv);
        std::cout << "bench=hnswlib rows=" << args.rows
                  << " dim=" << args.dim
                  << " queries=" << args.queries
                  << " truth_queries=" << args.truth_queries
                  << " k=" << args.k
                  << " m=" << args.m
                  << " ef_construction=" << args.ef_construction
                  << " ef_search=" << args.ef_search
                  << " threads=" << args.threads
                  << " seed=" << args.seed
                  << " clusters=" << args.clusters
                  << " noise=" << args.noise << std::endl;

        auto generate_start = clock_now();
        std::vector<float> data = generate_vectors(args);
        double generate_s = elapsed_seconds(generate_start);

        hnswlib::L2Space space(static_cast<int>(args.dim));

        // Sustained write: rebuild a fresh index in a loop for `insert_seconds`
        // (or a single build when 0) so throughput reflects steady-state, not a
        // burst. The last index is retained for the query phase.
        auto insert_start = clock_now();
        size_t insert_passes = 0;
        double insert_core = 0.0;
        std::unique_ptr<hnswlib::HierarchicalNSW<float>> index;
        do {
            index = std::make_unique<hnswlib::HierarchicalNSW<float>>(
                &space,
                args.rows,
                args.m,
                args.ef_construction,
                static_cast<size_t>(args.seed));
            index->setEf(args.ef_search);
            auto core = clock_now();
            parallel_for(0, args.rows, args.threads, [&](size_t row, size_t) {
                index->addPoint(static_cast<void *>(data.data() + row * args.dim), row);
            });
            insert_core += elapsed_seconds(core);
            ++insert_passes;
        } while (args.insert_seconds > 0.0 && elapsed_seconds(insert_start) < args.insert_seconds);
        double insert_s = elapsed_seconds(insert_start);
        double insert_qps = static_cast<double>(args.rows * insert_passes) / insert_s;
        // insert_core excludes index allocation + destruction.
        double insert_core_s = insert_core;
        double insert_core_qps = static_cast<double>(args.rows * insert_passes) / insert_core_s;

        // Sustained read: run the query workload in a loop for `query_seconds`
        // (or `query_repeats` passes when 0).
        std::vector<size_t> queries = query_ids(args, args.queries);
        auto query_start = clock_now();
        std::atomic<size_t> hits{0};
        size_t query_passes = 0;
        for (;;) {
            hits.store(0, std::memory_order_relaxed);
            parallel_for(0, queries.size(), args.threads, [&](size_t idx, size_t) {
                size_t row = queries[idx];
                std::priority_queue<std::pair<float, hnswlib::labeltype>> result =
                    index->searchKnn(data.data() + row * args.dim, args.k);
                bool found = false;
                while (!result.empty()) {
                    if (static_cast<size_t>(result.top().second) == row) {
                        found = true;
                        break;
                    }
                    result.pop();
                }
                if (found) {
                    hits.fetch_add(1, std::memory_order_relaxed);
                }
            });
            ++query_passes;
            if (args.query_seconds > 0.0) {
                if (elapsed_seconds(query_start) >= args.query_seconds) {
                    break;
                }
            } else if (query_passes >= args.query_repeats) {
                break;
            }
        }
        double query_s = elapsed_seconds(query_start);
        double query_qps = static_cast<double>(args.queries * query_passes) / query_s;
        double self_recall = static_cast<double>(hits.load()) / static_cast<double>(args.queries);

        std::vector<size_t> truth_queries = query_ids(args, args.truth_queries);
        auto truth_start = clock_now();
        std::atomic<size_t> recall_hits{0};
        parallel_for(0, truth_queries.size(), args.threads, [&](size_t idx, size_t) {
            size_t row = truth_queries[idx];
            const float *query = data.data() + row * args.dim;
            std::vector<size_t> truth = exact_top_k(query, data, args);
            std::priority_queue<std::pair<float, hnswlib::labeltype>> result =
                index->searchKnn(query, args.k);
            while (!result.empty()) {
                size_t id = static_cast<size_t>(result.top().second);
                for (size_t truth_id : truth) {
                    if (truth_id == id) {
                        recall_hits.fetch_add(1, std::memory_order_relaxed);
                        break;
                    }
                }
                result.pop();
            }
        });
        double truth_s = elapsed_seconds(truth_start);
        double recall_at_k = static_cast<double>(recall_hits.load())
            / static_cast<double>(args.truth_queries * args.k);

        std::cout << "result impl=hnswlib rows=" << args.rows
                  << " dim=" << args.dim
                  << " generate_s=" << generate_s
                  << " insert_s=" << insert_s
                  << " insert_passes=" << insert_passes
                  << " insert_qps=" << insert_qps
                  << " insert_core_s=" << insert_core_s
                  << " insert_core_qps=" << insert_core_qps
                  << " query_s=" << query_s
                  << " query_passes=" << query_passes
                  << " query_qps=" << query_qps
                  << " truth_s=" << truth_s
                  << " recall_at_" << args.k << "=" << recall_at_k
                  << " self_recall_at_" << args.k << "=" << self_recall
                  << std::endl;
        std::cout << "{\"impl\":\"hnswlib\",\"rows\":" << args.rows
                  << ",\"dim\":" << args.dim
                  << ",\"queries\":" << args.queries
                  << ",\"truth_queries\":" << args.truth_queries
                  << ",\"k\":" << args.k
                  << ",\"m\":" << args.m
                  << ",\"ef_construction\":" << args.ef_construction
                  << ",\"ef_search\":" << args.ef_search
                  << ",\"threads\":" << args.threads
                  << ",\"generate_s\":" << generate_s
                  << ",\"insert_s\":" << insert_s
                  << ",\"insert_qps\":" << insert_qps
                  << ",\"query_s\":" << query_s
                  << ",\"query_qps\":" << query_qps
                  << ",\"truth_s\":" << truth_s
                  << ",\"recall_at_k\":" << recall_at_k
                  << ",\"self_recall_at_k\":" << self_recall << "}"
                  << std::endl;
        return 0;
    } catch (const std::exception &e) {
        std::cerr << "ERROR: " << e.what() << std::endl;
        return 1;
    }
}

Args parse_args(int argc, char **argv) {
    Args args;
    for (int i = 1; i < argc; i += 2) {
        if (i + 1 >= argc) {
            throw std::invalid_argument(std::string("missing value for argument ") + argv[i]);
        }
        std::string flag(argv[i]);
        std::string value(argv[i + 1]);
        if (flag == "--rows") {
            args.rows = parse_size(value);
        } else if (flag == "--dim") {
            args.dim = parse_size(value);
        } else if (flag == "--queries") {
            args.queries = parse_size(value);
        } else if (flag == "--truth-queries") {
            args.truth_queries = parse_size(value);
        } else if (flag == "--k") {
            args.k = parse_size(value);
        } else if (flag == "--m") {
            args.m = parse_size(value);
        } else if (flag == "--ef-construction") {
            args.ef_construction = parse_size(value);
        } else if (flag == "--ef-search") {
            args.ef_search = parse_size(value);
        } else if (flag == "--threads") {
            args.threads = parse_size(value);
        } else if (flag == "--seed") {
            args.seed = parse_u64(value);
        } else if (flag == "--clusters") {
            args.clusters = parse_size(value);
        } else if (flag == "--noise") {
            args.noise = std::stof(value);
        } else if (flag == "--query-repeats") {
            args.query_repeats = parse_size(value);
        } else if (flag == "--insert-seconds") {
            args.insert_seconds = std::stod(value);
        } else if (flag == "--query-seconds") {
            args.query_seconds = std::stod(value);
        } else {
            throw std::invalid_argument("unknown argument: " + flag);
        }
    }
    if (args.rows == 0 || args.dim == 0 || args.queries == 0 || args.truth_queries == 0 || args.k == 0) {
        throw std::invalid_argument("rows, dim, queries, truth_queries, and k must be greater than 0");
    }
    if (args.clusters == 0) {
        throw std::invalid_argument("clusters must be greater than 0");
    }
    return args;
}

size_t parse_size(const std::string &value) {
    return static_cast<size_t>(std::stoull(value));
}

uint64_t parse_u64(const std::string &value) {
    return static_cast<uint64_t>(std::stoull(value));
}

Clock::time_point clock_now() {
    return Clock::now();
}

double elapsed_seconds(Clock::time_point start) {
    return std::chrono::duration<double>(Clock::now() - start).count();
}

std::vector<float> generate_vectors(const Args &args) {
    size_t total = args.rows * args.dim;
    if (args.rows != 0 && total / args.rows != args.dim) {
        throw std::overflow_error("rows * dim overflow");
    }
    std::vector<float> values(total);
    parallel_for(0, args.rows, args.threads, [&](size_t row, size_t) {
        float *out = values.data() + row * args.dim;
        for (size_t col = 0; col < args.dim; ++col) {
            out[col] = vector_value(row, col, args);
        }
    });
    return values;
}

std::vector<size_t> query_ids(const Args &args) {
    return query_ids(args, args.queries);
}

std::vector<size_t> query_ids(const Args &args, size_t count) {
    std::vector<size_t> ids(count);
    for (size_t idx = 0; idx < count; ++idx) {
        ids[idx] = static_cast<size_t>(
            splitmix64(args.seed ^ (static_cast<uint64_t>(idx) * UINT64_C(0x9e3779b97f4a7c15)))
            % static_cast<uint64_t>(args.rows));
    }
    return ids;
}

std::vector<size_t> exact_top_k(const float *query, const std::vector<float> &data, const Args &args) {
    std::priority_queue<std::pair<float, size_t>> heap;
    for (size_t row = 0; row < args.rows; ++row) {
        float distance = l2_distance(query, data.data() + row * args.dim, args.dim);
        if (heap.size() < args.k) {
            heap.emplace(distance, row);
        } else if (distance < heap.top().first) {
            heap.pop();
            heap.emplace(distance, row);
        }
    }
    std::vector<std::pair<float, size_t>> ordered;
    ordered.reserve(heap.size());
    while (!heap.empty()) {
        ordered.push_back(heap.top());
        heap.pop();
    }
    std::sort(ordered.begin(), ordered.end());
    std::vector<size_t> ids;
    ids.reserve(ordered.size());
    for (const auto &item : ordered) {
        ids.push_back(item.second);
    }
    return ids;
}

float l2_distance(const float *left, const float *right, size_t dim) {
    float total = 0.0f;
    for (size_t col = 0; col < dim; ++col) {
        float delta = left[col] - right[col];
        total += delta * delta;
    }
    return total;
}

float vector_value(size_t row, size_t col, const Args &args) {
    size_t cluster = row % args.clusters;
    uint64_t base_key = args.seed
        ^ (static_cast<uint64_t>(cluster) * UINT64_C(0xbf58476d1ce4e5b9))
        ^ (static_cast<uint64_t>(col) * UINT64_C(0x94d049bb133111eb));
    uint64_t noise_key = args.seed
        ^ (static_cast<uint64_t>(row) * UINT64_C(0x9e3779b97f4a7c15))
        ^ (static_cast<uint64_t>(col) * UINT64_C(0xd2b74407b1ce6e93));
    float base = unit_f32(base_key) * 2.0f - 1.0f;
    float noise = (unit_f32(noise_key) * 2.0f - 1.0f) * args.noise;
    return base + noise;
}

float unit_f32(uint64_t key) {
    uint64_t bits = splitmix64(key) >> 40;
    return static_cast<float>(bits) * (1.0f / 16777216.0f);
}

uint64_t splitmix64(uint64_t x) {
    x += UINT64_C(0x9e3779b97f4a7c15);
    uint64_t z = x;
    z = (z ^ (z >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)) * UINT64_C(0x94d049bb133111eb);
    return z ^ (z >> 31);
}

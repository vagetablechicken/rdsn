// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <fmt/format.h>
#include <chrono>
#include <atomic>

namespace dsn {
namespace stats {

// Take measurements and maintain a histogram.
struct histogram
{
    // Thread-Safe
    void measure(std::chrono::microseconds latency)
    {
        _count.store(_count.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);

        auto latency_us = static_cast<uint64_t>(latency.count());
        uint64_t index = latency_us / 10;
        if (index > bucket_capacity) {
            index = bucket_capacity;
        }
        _buckets[index].store(_buckets[index].load(std::memory_order_relaxed) + 1,
                              std::memory_order_relaxed);

        _sum.store(_sum.load(std::memory_order_relaxed) + latency_us, std::memory_order_relaxed);

        uint64_t old_min = min();
        if (latency_us < old_min) {
            _min.store(latency_us, std::memory_order_relaxed);
        }

        uint64_t old_max = max();
        if (latency_us > old_max) {
            _max.store(latency_us, std::memory_order_relaxed);
        }
    }

    // Thread-Safe
    std::string summary()
    {
        uint64_t minv = min();
        uint64_t maxv = max();
        uint64_t countv = count();
        uint64_t sumv = sum();
        auto avg = (uint64_t)((double)sumv / (double)countv);

        uint64_t p90{0};
        uint64_t p99{0};
        uint64_t p999{0};
        uint64_t p9999{0};

        uint64_t total = 0;
        for (uint64_t i = 0; i < bucket_capacity + 1; i++) {
            total += _buckets[i].load(std::memory_order_relaxed);

            auto per = static_cast<double>(total) / static_cast<double>(countv);
            if (p90 == 0 && per >= 0.90) {
                p90 = i * 100;
            }
            if (p99 == 0 && per >= 0.99) {
                p99 = i * 100;
            }
            if (p999 == 0 && per >= 0.999) {
                p999 = i * 100;
            }
            if (p9999 == 0 && per >= 0.9999) {
                p9999 = i * 100;
            }
        }

        return fmt::format(
            "count={},P90={}us, P99={}us, P999={}us, P9999={}us, max={}us, min={}, avg={}us",
            countv,
            p90,
            p99,
            p999,
            p9999,
            minv,
            maxv,
            avg);
    }

private:
    uint64_t min() const { return _min.load(std::memory_order_relaxed); }

    uint64_t max() const { return _max.load(std::memory_order_relaxed); }

    uint64_t sum() const { return _sum.load(std::memory_order_relaxed); }

    uint64_t count() const { return _count.load(std::memory_order_relaxed); }

private:
    // from 100 us to 100 ms, bucket[0] for [0, 10us), bucket[1] for [10us, 20us) ...
    // TODO(wutao1): make capacity configurable.
    static constexpr int bucket_capacity = 1000;
    std::atomic_uint_fast64_t _buckets[bucket_capacity + 1];
    std::atomic_uint_fast64_t _min{0};
    std::atomic_uint_fast64_t _max{0};
    std::atomic_uint_fast64_t _sum{0};
    std::atomic_uint_fast64_t _count{0};
};

} // namespace stats
} // namespace dsn

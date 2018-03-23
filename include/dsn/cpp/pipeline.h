/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#pragma once

#include <dsn/tool-api/task_code.h>
#include <dsn/cpp/clientlet.h>
#include <dsn/utility/chrono_literals.h>

namespace dsn {
namespace pipeline {

struct pipeline_option
{
    task_code thread_pool_code;
    clientlet *task_tracker{nullptr};
    int thread_hash{0};
};

template <typename F>
struct segment
{
    using return_type = typename std::result_of<F>::type;
    std::function<F> func;

    segment(F &&f) : func(std::forward<F>(f)) {}

    template <typename... Args>
    return_type run(Args... args)
    {
        return func();
    }
};

template <typename T, typename F>
segment<F>::return_type operator|(T &&x, segment<F> &seg)
{
    return seg.func(x);
}

template <typename Input, typename Mid, typename Output>
segment<Output (*)(Input)> operator|(segment<Mid(Input)> &l, segment<Output(Mid)> &r)
{
    return {[l, r](Input in) -> Output { return r.func(l.func(in)); }};
}

struct pipeline_base
{
    pipeline_option opt;

    pipeline_base &thread_pool(task_code tc)
    {
        opt.thread_pool_code = tc;
        return *this;
    }

    pipeline_base &task_tracker(clientlet *tracker)
    {
        opt.task_tracker = tracker;
        return *this;
    }

    pipeline_base &thread_hash(int hash)
    {
        opt.thread_hash = hash;
        return *this;
    }
};

} // namespace pipeline
} // namespace dsn

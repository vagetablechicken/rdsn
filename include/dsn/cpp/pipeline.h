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
#include <dsn/utility/apply.h>

namespace dsn {
namespace pipeline {

// The environment for execution.
struct environment
{
    template <typename F>
    void schedule(F &&f, std::chrono::milliseconds delay_ms = 0_ms)
    {
        tasking::enqueue(__conf.thread_pool_code,
                         __conf.task_tracker,
                         std::forward<F>(f),
                         __conf.thread_hash,
                         delay_ms);
    }

    struct
    {
        task_code thread_pool_code;
        clientlet *task_tracker{nullptr};
        int thread_hash{0};
    } __conf;
};

template <typename... Args>
struct result
{
    typedef std::tuple<Args...> ArgsTupleType;

    void step_down_next_stage(Args &&... args)
    {
        func(std::make_tuple(std::forward<Args>(args)...));
    }

    std::function<void(ArgsTupleType &&)> func;
};

struct base : environment
{
    virtual ~base()
    {
        pause();

        if (__conf.task_tracker != nullptr) {
            wait_all();
        }
    }

    void run_pipeline();

    void pause() { _paused.store(true, std::memory_order_release); }

    bool paused() { return _paused.load(std::memory_order_acquire); }

    // Await for all running tasks to complete.
    void wait_all() { dsn_task_tracker_wait_all(__conf.task_tracker->tracker()); }

    /// === Environment Configuration === ///

    base &thread_pool(task_code tc)
    {
        __conf.thread_pool_code = tc;
        return *this;
    }
    base &thread_hash(int hash)
    {
        __conf.thread_hash = hash;
        return *this;
    }
    base &task_tracker(clientlet *tracker)
    {
        __conf.task_tracker = tracker;
        return *this;
    }

    /// === Pipeline Declaration === ///

    template <typename Stage>
    struct pipeline_node
    {
        template <typename NextStage>
        pipeline_node<NextStage> link(NextStage *next)
        {
            using ArgsTupleType = typename Stage::ArgsTupleType;

            next->__conf = this_stage->__conf;
            next->__pipeline = this_stage->__pipeline;
            this_stage->func = [next](ArgsTupleType &&args) mutable {
                if (next->paused()) {
                    return;
                }
                dsn::apply(&NextStage::run, std::tuple_cat(std::make_tuple(next), std::move(args)));
            };
            return {next};
        }

        /// Link to stage of another pipeline.
        template <typename NextStage>
        void link_pipe(NextStage *next)
        {
            // lazily get the result type of this stage,
            // since it's probable not inherited from result<>.
            using ArgsTupleType = typename Stage::ArgsTupleType;

            this_stage->func = [next](ArgsTupleType &&args) mutable {
                dsn::apply(&NextStage::run, std::tuple_cat(std::make_tuple(next), std::move(args)));
            };
        }

        pipeline_node(Stage *s) : this_stage(s) {}

    private:
        Stage *this_stage;
    };

    template <typename Stage>
    pipeline_node<Stage> from(Stage *start)
    {
        start->__conf = __conf;
        start->__pipeline = this;
        _root_stage = start;
        return {start};
    }

private:
    environment *_root_stage{nullptr};
    std::atomic_bool _paused{true};
};

// A piece of execution, receiving argument `Input`, running in the environment
// created by `pipeline::base`.
template <typename... Args>
struct when : environment
{
    virtual void run(Args &&... in) = 0;

    void repeat(Args &&... in, std::chrono::milliseconds delay_ms = 0_ms)
    {
        auto arg_tuple = std::make_tuple(this, std::forward<Args>(in)...);
        schedule([ this, args = std::move(arg_tuple) ]() mutable {
            if (paused()) {
                return;
            }
            dsn::apply(&when<Args...>::run, args);
        },
                 delay_ms);
    }

    bool paused() { return __pipeline->paused(); }

private:
    friend class base;

    base *__pipeline;
};

inline void base::run_pipeline()
{
    _paused.store(false, std::memory_order_release);

    schedule([stage = static_cast<when<> *>(_root_stage)]() {
        // static_cast for downcast, but completely safe.
        stage->run();
    });
}

template <typename... Args>
struct mock_when : when<Args...>
{
    explicit mock_when(std::function<void(Args &&... args)> &&func) : _cb(std::move(func)) {}

    void run(Args &&... args) override { _cb(std::forward<Args>(args)...); }

private:
    std::function<void(Args &&...)> _cb;
};

} // namespace pipeline
} // namespace dsn

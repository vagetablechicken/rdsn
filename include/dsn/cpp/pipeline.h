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

struct pipeline_config
{
    task_code thread_pool_code;
    clientlet *task_tracker{nullptr};
    int thread_hash{0};
};

// The environment for execution.
struct environment
{
    template <typename F>
    void schedule(F &&f, std::chrono::milliseconds delay_ms = 0_ms)
    {
        tasking::enqueue(conf.thread_pool_code,
                         conf.task_tracker,
                         std::forward<F>(f),
                         conf.thread_hash,
                         delay_ms);
    }

protected:
    friend struct base;

    template <typename Stage>
    friend struct pipeline_node;

    pipeline_config conf;
};

namespace traits {

template <typename T>
struct input_type
{
    typedef typename T::input_type type;
};

template <typename T>
using input_type_t = typename input_type<T>::type;

template <typename T>
struct output_type
{
    typedef typename T::output_type type;
};

template <typename T>
using output_type_t = typename output_type<T>::type;

template <typename Stage, typename NextStage>
struct pipeline_match
{
    static constexpr bool value = std::is_same<output_type_t<Stage>, input_type_t<NextStage>>::value
};

template <typename Stage, typename NextStage>
static constexpr bool pipeline_match_v = pipeline_match<Stage, NextStage>::value;

} // namespace traits

template <typename Output>
struct result
{
    typedef Output output_type;

    void step_down_next_stage(Output &&out) { func(out); }

    std::function<void(Output &)> func;
};

struct result_0
{
    typedef void output_type;

    void step_down_next_stage() { func(); }

    std::function<void()> func;
};

// A piece of execution, receiving argument `Input`, running in the environment
// specified by `pipeline_config`.
template <typename Input>
struct when : environment
{
    typedef Input input_type;

    virtual void run(Input &in) = 0;

    void repeat(Input &in, std::chrono::milliseconds delay_ms = 0_ms)
    {
        schedule([ this, arg = std::move(in) ]() mutable { run(arg); }, delay_ms);
    }
};

// A special variant of when, executing without parameters.
struct when_0 : environment
{
    typedef void input_type;

    virtual void run() = 0;

    void repeat(std::chrono::milliseconds delay_ms = 0_ms)
    {
        schedule([this]() { run(); }, delay_ms);
    }
};

// A special variant of when.
// Executed as when<input>, but can be called by `run()` like when_0.
template <typename Input>
struct when_arg : when_0
{
    typedef Input input_type;

    void bind_arg(Input &in) { _arg = std::move(in); }

    Input &get_arg() { return _arg; }

    const Input &get_arg() const { return _arg; }

private:
    Input _arg;
};

// A special variant of when, which executes in parallel for each of
// the elements of `Container`.
template <typename Container, typename Input = typename Container::value_type>
struct parallel_when : when<Input>
{
    typedef Container input_type;

    void parallel_for_each(Container &c)
    {
        _pending = std::move(c);
        for (Input element : _pending) {
            when<Input>::run(element);
        }
    }

protected:
    Container _pending;
};

template <typename Stage>
struct pipeline_node
{
    typedef traits::output_type_t<Stage> ArgType;

    template <typename NextStage>
    pipeline_node<NextStage> link(NextStage *next)
    {
        static_assert(traits::pipeline_match_v<Stage, NextStage>, "");
        next->conf = this_stage->conf;

        this_stage->func = [next](ArgType &args) mutable { next->run(args); };
        return {next};
    }

    template <typename NextStage>
    pipeline_node<NextStage> link_0(NextStage *next)
    {
        static_assert(traits::pipeline_match_v<Stage, NextStage>, "");
        next->conf = this_stage->conf;

        this_stage->func = [next]() mutable { next->run(); };
        return {next};
    }

    template <typename NextStage>
    pipeline_node<NextStage> link_parallel(NextStage *next)
    {
        static_assert(traits::pipeline_match_v<Stage, NextStage>, "");
        next->conf = this_stage->conf;

        this_stage->func = [next](ArgType &args) mutable { next->parallel_for_each(args); };
        return {next};
    }

    pipeline_node(Stage *s) : this_stage(s) {}

private:
    Stage *this_stage;
};

struct base : environment
{
    template <typename Stage>
    pipeline_node<Stage> from(Stage *start)
    {
        start->conf = conf;
        _root_stage = start;
        return {start};
    }

    void run()
    {
        schedule([stage = static_cast<when_0 *>(_root_stage)]() {
            // static_cast for downcast, but completely safe.
            stage->run();
        });
    }

    template <typename Input>
    void run(Input &in)
    {
        schedule([ stage = static_cast<when_arg<Input> *>(_root_stage), in = std::move(in) ]() {
            stage->bind_arg(in);
            stage->run();
        });
    }

    // Await for all running tasks to complete.
    void wait_all() { dsn_task_tracker_wait_all(conf.task_tracker->tracker()); }

    /// === Environment Configuration === ///

    base &thread_pool(task_code tc)
    {
        conf.thread_pool_code = tc;
        return *this;
    }
    base &thread_hash(int hash)
    {
        conf.thread_hash = hash;
        return *this;
    }
    base &task_tracker(clientlet *tracker)
    {
        conf.task_tracker = tracker;
        return *this;
    }

private:
    environment *_root_stage{nullptr};
};

} // namespace pipeline
} // namespace dsn

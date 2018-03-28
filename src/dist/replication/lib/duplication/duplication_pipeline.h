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

#include <dsn/cpp/pipeline.h>
#include <dsn/dist/replication/duplication_backlog_handler.h>

#include "mutation_duplicator.h"

namespace dsn {
namespace replication {

class private_log_loader;

using namespace dsn::literals::chrono_literals;

struct load_mutation : pipeline::when<>, pipeline::result<decree, mutation_tuple_set>
{
    void run() override;

    /// ==== Implementation ==== ///

    explicit load_mutation(mutation_duplicator *duplicator);

    ~load_mutation();

    bool have_more() const
    {
        return _duplicator->_replica->private_log()->max_commit_on_disk() >= _start_decree;
    }

private:
    std::unique_ptr<private_log_loader> _log_on_disk;
    prepare_list *_log_in_cache{nullptr};
    decree _start_decree{0};
    mutation_tuple_set _loaded_mutations;

    mutation_duplicator *_duplicator{nullptr};
};

struct ship_mutation : pipeline::when<decree, mutation_tuple_set>, public pipeline::result<>
{
    void run(decree &&last_decree, mutation_tuple_set &&in) override
    {
        _last_decree = last_decree;
        _pending = std::move(in);
        for (mutation_tuple mut : _pending) {
            ship(mut);
        }
    }

    /// ==== Implementation ==== ///

    explicit ship_mutation(mutation_duplicator *duplicator) : _duplicator(duplicator)
    {
        _backlog_handler = new_backlog_handler(get_gpid(),
                                               _duplicator->remote_cluster_address(),
                                               _duplicator->_replica->get_app_info()->app_name);
    }

    void ship(mutation_tuple &mut);

    void repeat(mutation_tuple &mut, std::chrono::milliseconds delay_ms)
    {
        schedule([ this, mut = std::move(mut) ]() mutable { ship(mut); }, delay_ms);
    }

    gpid get_gpid() { return _duplicator->get_gpid(); }

private:
    friend class ship_mutation_test;

    std::unique_ptr<duplication_backlog_handler> _backlog_handler;

    mutation_duplicator *_duplicator{nullptr};
    mutation_tuple_set _pending;
    decree _last_decree{0};
};

} // namespace replication
} // namespace dsn

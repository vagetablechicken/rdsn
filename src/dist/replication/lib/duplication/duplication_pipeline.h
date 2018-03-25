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

#include "mutation_duplicator.h"

namespace dsn {
namespace replication {

class private_log_loader;

using namespace dsn::literals::chrono_literals;

struct load_mutation : pipeline::when_0, pipeline::result<mutation_tuple_set>
{
    void run() override;

    /// ==== Implementation ==== ///

    explicit load_mutation(mutation_duplicator *duplicator)
        : _log_in_cache(duplicator->_replica->_prepare_list),
          _start_decree(duplicator->_view->last_decree + 1)
    {
    }

    ~load_mutation();

    bool have_more() const
    {
        return _duplicator->_replica->private_log()->max_commit_on_disk() >= _start_decree;
    }

private:
    std::unique_ptr<private_log_loader> _log_on_disk;
    prepare_list *_log_in_cache;
    decree _start_decree;
    mutation_tuple_set _loaded_mutations;

    mutation_duplicator *_duplicator{nullptr};
};

struct ship_mutation : pipeline::parallel_when<mutation_tuple_set>, pipeline::result_0
{
    void run(mutation_tuple &) override;

    /// ==== Implementation ==== ///

    explicit ship_mutation(mutation_duplicator *duplicator) : _duplicator(duplicator)
    {
        _backlog_handler = new_backlog_handler(get_gpid(),
                                               _duplicator->remote_cluster_address(),
                                               _duplicator->_replica->get_app_info()->app_name);
    }

    gpid get_gpid() { return _duplicator->get_gpid(); }

private:
    std::unique_ptr<duplication_backlog_handler> _backlog_handler;

    mutation_duplicator *_duplicator{nullptr};
};

struct duplication_pipeline : pipeline::base
{
    explicit duplication_pipeline(mutation_duplicator *duplicator)
    {
        thread_pool(LPC_DUPLICATE_MUTATIONS)
            .task_tracker(duplicator->tracker())
            .thread_hash(duplicator->get_gpid().thread_hash());

        /// loop for loading when shipping finishes
        from(_load.get()).link_parallel(_ship.get()).link_0(_load.get());
    }

    std::unique_ptr<load_mutation> _load;
    std::unique_ptr<ship_mutation> _ship;
};

} // namespace replication
} // namespace dsn

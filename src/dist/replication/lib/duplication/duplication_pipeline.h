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
class mutation_duplicator;

using namespace dsn::literals::chrono_literals;

struct mutation_loader : pipeline::stage<mutation_loader, mutation_shipper>
{
    mutation_loader(mutation_duplicator *duplicator)
        : _log_in_cache(duplicator->_replica->_prepare_list),
          _start_decree(duplicator->_view->last_decree + 1)
    {
    }

    void process();

    /// ==== Implementation ==== ///

    bool have_more() const
    {
        return _duplicator->_replica->private_log()->max_commit_on_disk() >= _start_decree;
    }

    void add_mutation_if_valid(mutation_ptr &mu);

private:
    std::unique_ptr<private_log_loader> _log_on_disk;
    prepare_list *_log_in_cache;
    decree _start_decree;
    std::set<mutation_tuple> _loaded_mutations;

    mutation_duplicator *_duplicator{nullptr};
};

struct mutation_shipper : pipeline::stage<mutation_shipper, mutation_loader>
{
    void process(mutation_tuple &mutations);

    void process(std::set<mutation_tuple> &mutations)
    {
        for (mutation_tuple mut : mutations) {
            process(mut);
        }
    }

    gpid get_gpid() { return _duplicator->get_gpid(); }

private:
    std::unique_ptr<duplication_backlog_handler> _backlog_handler;

    mutation_duplicator *_duplicator{nullptr};
};

struct duplication_pipeline : pipeline::pipeline_base
{
    explicit duplication_pipeline(mutation_duplicator *duplicator)
    {
        thread_pool(LPC_DUPLICATE_MUTATIONS)
            .task_tracker(duplicator->tracker())
            .thread_hash(duplicator->get_gpid().thread_hash())
            .from(_loader.get())
            .link(_shipper.get())
            .link(_loader.get());
    }

    void run() {}

    std::unique_ptr<mutation_loader> _loader;
    std::unique_ptr<mutation_shipper> _shipper;
};

} // namespace replication
} // namespace dsn

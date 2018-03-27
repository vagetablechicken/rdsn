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

#include "duplication_pipeline.h"
#include "private_log_loader.h"

namespace dsn {
namespace replication {

void load_mutation::run()
{
    if (!have_more()) {
        // wait 10 seconds for next try if no mutation was added.
        repeat(10_s);
        return;
    }

    // try load from cache
    if (_start_decree >= _log_in_cache->min_decree()) {
        for (decree d = _start_decree; d <= _log_in_cache->last_committed_decree(); d++) {
            auto mu = _log_in_cache->get_mutation_by_decree(d);
            dassert(mu != nullptr, "");

            add_mutation_if_valid(mu, _loaded_mutations);
        }

        step_down_next_stage(std::move(_loaded_mutations));
        return;
    }

    // load from private log
    _log_on_disk->load_mutations_from_decree(_start_decree);
}

load_mutation::~load_mutation() {}

load_mutation::load_mutation(mutation_duplicator *duplicator)
    : _log_on_disk(new private_log_loader(duplicator)),
      _log_in_cache(duplicator->_replica->_prepare_list),
      _start_decree(duplicator->_view->last_decree + 1)
{
}

void ship_mutation::ship(mutation_tuple &mut)
{
    _backlog_handler->duplicate(mut, [this, mut](error_s err) mutable {
        uint64_t ts = std::get<0>(mut);

        if (!err.is_ok()) {
            derror_replica("failed to ship mutation: {} to {}, timestamp: {}",
                           err,
                           _duplicator->remote_cluster_address(),
                           ts);

            // retry infinitely whenever error occurs.
            // delay 1 sec for retry.
            repeat(mut, 1_s);
            return;
        }

        schedule([mut, this]() {
            _pending.erase(mut);

            if (_pending.empty()) {
                step_down_next_stage();
            }
        });
    });
}

} // namespace replication
} // namespace dsn

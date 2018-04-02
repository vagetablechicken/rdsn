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

#include <dsn/dist/replication/replication_app_base.h>

#include "duplication_pipeline.h"
#include "load_from_private_log.h"

namespace dsn {
namespace replication {

void load_mutation::run()
{
    decree max_gced_decree = _replica->private_log()->max_gced_decree(
        _replica->get_gpid(), _replica->get_app()->init_info().init_offset_in_private_log);
    if (max_gced_decree > _duplicator->_view->confirmed_decree) {
        dfatal_replica("the logs haven't yet duplicated were accidentally truncated "
                       "[last_durable_decree: {}, confirmed_decree: {}]",
                       _replica->last_durable_decree(),
                       _duplicator->_view->confirmed_decree);
        __builtin_unreachable();
    }

    _start_decree = _duplicator->_view->last_decree + 1;

    if (!have_more()) {
        // wait 10 seconds for next try if no mutation was added.
        repeat(10_s);
        return;
    }

    //    // try load from cache
    //    if (_start_decree >= _log_in_cache->min_decree()) {
    //        for (decree d = _start_decree; d <= _log_in_cache->last_committed_decree(); d++) {
    //            auto mu = _log_in_cache->get_mutation_by_decree(d);
    //            dassert(mu != nullptr, "");
    //
    //            add_mutation_if_valid(mu, _loaded_mutations);
    //        }
    //
    //        if(_loaded_mutations.empty()) {
    //            repeat(10_s);
    //            return;
    //        }
    //
    //        step_down_next_stage(_log_in_cache->last_committed_decree(),
    //        std::move(_loaded_mutations));
    //        return;
    //    }

    // load from private log
    _log_on_disk->set_start_decree(_start_decree);
    _log_on_disk->async();
}

load_mutation::~load_mutation() = default;

load_mutation::load_mutation(mutation_duplicator *duplicator, replica *r)
    : _log_on_disk(new load_from_private_log(r)),
      _log_in_cache(r->_prepare_list),
      _replica(r),
      _duplicator(duplicator)
{
    // load from on-disk private log -> ship via duplicator
    duplicator->fork(*_log_on_disk, LPC_DUPLICATION_LOAD_MUTATIONS, 0)
        .link_pipe(*duplicator->_ship);
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
                _duplicator->update_state(duplication_view().set_last_decree(_last_decree));
                step_down_next_stage();
            }
        });
    });
}

} // namespace replication
} // namespace dsn

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

#include "mutation_duplicator.h"
#include "mutation_loader.h"

#include "dist/replication/lib/replica.h"

#include <dsn/dist/replication/replication_app_base.h>

namespace dsn {
namespace replication {

mutation_duplicator::mutation_duplicator(const duplication_entry &ent, replica *r)

    : _id(ent.dupid),
      _remote_cluster_address(ent.remote_address),
      _replica(r),
      _paused(true),
      _view(make_unique<duplication_view>())
{
    _backlog_handler = new_backlog_handler(
        get_gpid(), _remote_cluster_address, _replica->get_app_info()->app_name);

    _loader = dsn::make_unique<mutation_loader>(this);

    _view->confirmed_decree = ent.confirmed_decree;
    _view->status = ent.status;

    // start from the last confirmed decree
    _view->last_decree = ent.confirmed_decree;
}

mutation_duplicator::~mutation_duplicator()
{
    pause();
    wait_all();
}

void mutation_duplicator::start()
{
    ddebug_replica("starting duplication [dupid: {}, remote: {}]", id(), remote_cluster_address());

    decree max_gced_decree = _replica->private_log()->max_gced_decree(
        get_gpid(), _replica->get_app()->init_info().init_offset_in_private_log);
    if (max_gced_decree > _view->confirmed_decree) {
        dfatal_replica("the logs haven't yet duplicated were accidentally truncated "
                       "[last_durable_decree: {}, confirmed_decree: {}]",
                       _replica->last_durable_decree(),
                       _view->confirmed_decree);
    }

    _paused = false;
    enqueue_do_duplication();
}

void mutation_duplicator::pause()
{
    tasking::enqueue(LPC_DUPLICATE_MUTATIONS,
                     tracker(),
                     [this]() {
                         _paused = true;
                         _loader->pause();
                     },
                     get_gpid().thread_hash());
}

void mutation_duplicator::do_duplicate()
{
    if (_paused) {
        return;
    }

    if (!have_more()) {
        // wait 10 seconds for next try if no mutation was added.
        enqueue_do_duplication(10_s);
        return;
    }

    // will call on_mutations_loaded on success.
    _loader->load_mutations();
}

void mutation_duplicator::ship_mutations()
{
    if (_paused) {
        return;
    }

    mutation_tuple_set pending_mutations = _pending_mutations; // copy to prevent interleaving
    for (mutation_tuple mut : pending_mutations) {
        loop_to_duplicate(std::move(mut));
    }
}

void mutation_duplicator::loop_to_duplicate(mutation_tuple mut)
{
    if (_paused) {
        return;
    }

    _backlog_handler->duplicate(mut, [this, mut](error_s err) {
        if (!err.is_ok()) {
            derror_replica("failed to ship mutation: {} to {}, timestamp: {}",
                           err,
                           remote_cluster_address(),
                           std::get<0>(mut));
        }

        if (err.is_ok()) {
            // impose a lock here since there may have multiple tasks
            // erasing elements in the pending set.
            ::dsn::service::zauto_lock _(_pending_lock);
            _pending_mutations.erase(mut);

            if (_pending_mutations.empty()) {
                duplication_view new_state = view().set_last_decree(_mutation_batch->last_decree());
                update_state(new_state);

                // delay 1 second to start next duplication job
                enqueue_do_duplication(1_s);
            }
        } else {
            // retry infinitely whenever error occurs.
            // delay 1 sec for retry.
            enqueue_loop_to_duplicate(mut, 1_s);
        }
    });
}

} // namespace replication
} // namespace dsn

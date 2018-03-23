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
#include "private_log_loader.h"
#include "mutation_batch.h"

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

    auto it = ent.progress.find(get_gpid().get_partition_index());
    if (it != ent.progress.end()) {
        _view->last_decree = _view->confirmed_decree = it->second;
    }

    _view->status = ent.status;
}

mutation_duplicator::~mutation_duplicator()
{
    pause();
    wait_all();
}

void mutation_duplicator::start()
{
    tasking::enqueue(
        LPC_DUPLICATE_MUTATIONS,
        tracker(),
        [this]() {
            ddebug_replica(
                "starting duplication [dupid: {}, remote: {}]", id(), remote_cluster_address());
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
        },
        get_gpid().thread_hash());
}

void mutation_duplicator::pause()
{
    tasking::enqueue(
        LPC_DUPLICATE_MUTATIONS, tracker(), [this]() { _paused = true; }, get_gpid().thread_hash());
}

void mutation_loaded_listener::notify(mutation_tuple &mu)
{
    tasking::enqueue(LPC_DUPLICATE_MUTATIONS,
                     _duplicator->tracker(),
                     [ this, mu = std::move(mu) ]() {
                         for (mutation_update &update : mu->data.updates) {
                             // ignore WRITE_EMPTY (heartbeat)
                             if (update.code == RPC_REPLICATION_WRITE_EMPTY) {
                                 continue;
                             }

                             dsn_message_t req = from_blob_to_received_msg(
                                 update.code,
                                 update.data,
                                 0,
                                 0,
                                 dsn_msg_serialize_format(update.serialization_type));

                             _duplicated_listener->listen(mu->get_decree());
                         }

                     },
                     _duplicator->get_gpid().thread_hash());
}

void mutation_duplicated_listener::notify(decree d)
{
    _pending_mutations.erase(d);

    tasking::enqueue(LPC_DUPLICATE_MUTATIONS,
                     _duplicator->tracker(),
                     [this]() {
                         if (_pending_mutations.empty()) {
                             _duplicator->enqueue_do_duplication();
                         }
                     },
                     _duplicator->get_gpid().thread_hash());
}

void mutation_duplicated_listener::listen(decree d) { _pending_mutations.insert(d); }

} // namespace replication
} // namespace dsn

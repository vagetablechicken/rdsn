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

#include <dsn/dist/replication/duplication_common.h>
#include <dsn/dist/replication/duplication_backlog_handler.h>
#include <dsn/utility/chrono_literals.h>

#include "dist/replication/lib/replica.h"

namespace dsn {
namespace replication {

using namespace dsn::literals::chrono_literals;

class duplication_view
{
public:
    // the maximum decree that's been persisted in meta server
    decree confirmed_decree{0};

    // the maximum decree that's been duplicated to remote.
    decree last_decree{0};

    duplication_status::type status{duplication_status::DS_INIT};

    duplication_view &set_last_decree(decree d)
    {
        last_decree = d;
        return *this;
    }

    duplication_view &set_confirmed_decree(decree d)
    {
        confirmed_decree = d;
        return *this;
    }
};

typedef std::unique_ptr<duplication_view> duplication_view_u_ptr;

class mutation_loader;
class mutation_batch;

// Each mutation_duplicator is responsible for one duplication.
// It works in THREAD_POOL_REPLICATION (LPC_DUPLICATE_MUTATIONS),
// sharded by gpid, so that all functions are single-threaded,
// no lock required.
//
// TODO(wutao1): optimize
// Currently we create duplicator for every duplication.
// They're isolated even if they share the same private log.
class mutation_duplicator
{
public:
    mutation_duplicator(const duplication_entry &ent, replica *r);

    // This is a blocking call.
    // The thread may be seriously blocked under the destruction.
    // Take care when it runs in THREAD_POOL_REPLICATION, though generally
    // duplication removal is extremely rare.
    ~mutation_duplicator();

    // Thread-safe
    void start();

    // Thread-safe
    void pause();

    dupid_t id() const { return _id; }

    const std::string &remote_cluster_address() const { return _remote_cluster_address; }

    // Thread-safe
    duplication_view view() const
    {
        ::dsn::service::zauto_read_lock l(_lock);
        return *_view;
    }

    // Thread-safe
    void update_state(const duplication_view &new_state)
    {
        ::dsn::service::zauto_write_lock l(_lock);
        _view->confirmed_decree = std::max(_view->confirmed_decree, new_state.confirmed_decree);
        _view->last_decree = std::max(_view->last_decree, new_state.last_decree);
        _view->status = new_state.status;
    }

    /// ================================= Implementation =================================== ///

    // Await for all running tasks to complete.
    void wait_all() { dsn_task_tracker_wait_all(tracker()->tracker()); }

    gpid get_gpid() { return _replica->get_gpid(); }

    bool have_more() const
    {
        return _replica->private_log()->max_commit_on_disk() > _view->last_decree;
    }

    void enqueue_do_duplication(std::chrono::milliseconds delay_ms = 0_ms)
    {
        if (_paused) {
            return;
        }

        tasking::enqueue(LPC_DUPLICATE_MUTATIONS,
                         tracker(),
                         std::bind(&mutation_duplicator::do_duplicate, this),
                         get_gpid().thread_hash(),
                         delay_ms);
    }

    void do_duplicate();

    void enqueue_ship_mutations(std::chrono::milliseconds delay_ms = 0_ms)
    {
        if (_paused) {
            return;
        }

        tasking::enqueue(LPC_DUPLICATE_MUTATIONS,
                         tracker(),
                         std::bind(&mutation_duplicator::ship_mutations, this),
                         get_gpid().thread_hash(),
                         delay_ms);
    }

    void ship_mutations();

    void enqueue_loop_to_duplicate(mutation_tuple mut, std::chrono::milliseconds delay_ms = 0_ms)
    {
        if (_paused) {
            return;
        }

        tasking::enqueue(LPC_DUPLICATE_MUTATIONS,
                         tracker(),
                         [ mut = std::move(mut), this ]() { loop_to_duplicate(mut); },
                         get_gpid().thread_hash(),
                         delay_ms);
    }

    void loop_to_duplicate(mutation_tuple mut);

private:
    // Returns: the task tracker.
    clientlet *tracker() { return _replica; }

private:
    friend class mutation_duplicator_test;
    friend class mutation_loader;

    const dupid_t _id;
    const std::string _remote_cluster_address;

    replica *_replica;

    bool _paused;

    std::unique_ptr<mutation_loader> _loader;
    std::unique_ptr<duplication_backlog_handler> _backlog_handler;

    mutable ::dsn::service::zauto_lock _pending_lock;
    mutation_tuple_set _pending_mutations;
    decree _last_prepared_decree;

    // protect the access of _view.
    mutable ::dsn::service::zrwlock_nr _lock;
    duplication_view_u_ptr _view;
};

typedef std::unique_ptr<mutation_duplicator> mutation_duplicator_u_ptr;

} // namespace replication
} // namespace dsn

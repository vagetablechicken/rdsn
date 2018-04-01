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
#include <dsn/cpp/pipeline.h>

#include "dist/replication/lib/replica.h"

namespace dsn {
namespace replication {

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

struct load_mutation;
struct ship_mutation;
struct load_from_private_log;

// Each mutation_duplicator is responsible for one duplication.
// It works in THREAD_POOL_REPLICATION (LPC_DUPLICATE_MUTATIONS),
// sharded by gpid, so, all functions are single-threaded,
// no read lock required (of course write lock is necessary when
// reader could be in other thread).
//
// TODO(wutao1): optimize
// Currently we create duplicator for every duplication.
// They're isolated even if they share the same private log.
struct mutation_duplicator : pipeline::base
{
public:
    mutation_duplicator(const duplication_entry &ent, replica *r);

    // This is a blocking call.
    // The thread may be seriously blocked under the destruction.
    // Take care when running in THREAD_POOL_REPLICATION, though
    // duplication removal is extremely rare.
    ~mutation_duplicator();

    // Thread-safe
    void start();

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
        if (new_state.confirmed_decree != 0) {
            _view->confirmed_decree = std::max(_view->confirmed_decree, new_state.confirmed_decree);
        }
        if (new_state.last_decree != 0) {
            _view->last_decree = std::max(_view->last_decree, new_state.last_decree);
        }
        if (new_state.status != duplication_status::DS_INIT) {
            _view->status = new_state.status;
        }
    }

    gpid get_gpid() { return _replica->get_gpid(); }

    // Use replica as task tracker, mutation_duplicator is bound to be destroyed
    // before its replica.
    // Returns: the task tracker.
    clientlet *tracker() { return _replica; }

private:
    friend class mutation_duplicator_test;

    friend struct load_mutation;
    friend struct ship_mutation;

    const dupid_t _id;
    const std::string _remote_cluster_address;

    replica *_replica;

    // protect the access of _view.
    mutable ::dsn::service::zrwlock_nr _lock;
    duplication_view_u_ptr _view;

    /// === pipeline === ///
    std::unique_ptr<load_mutation> _load;
    std::unique_ptr<ship_mutation> _ship;
};

typedef std::unique_ptr<mutation_duplicator> mutation_duplicator_u_ptr;

} // namespace replication
} // namespace dsn

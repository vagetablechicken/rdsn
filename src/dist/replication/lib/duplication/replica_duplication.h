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

#include "mutation_duplicator.h"

#include <dsn/dist/replication/replication_types.h>
#include <dsn/dist/replication/duplication_common.h>

#include "dist/replication/lib/replica.h"
#include "dist/replication/lib/mutation_log.h"

namespace dsn {
namespace replication {

// duplication_impl manages the set of duplications on this replica.
// This class is not thread-safe.
class replica::duplication_impl
{
public:
    explicit duplication_impl(replica *r) : _replica(r) {}

    // Start a new duplication if there's no dup with `dupid`, or it will change
    // the dup to `next_status`.
    void sync_duplication(const duplication_entry &ent);

    // collect updated duplication confirm points from this node.
    std::vector<duplication_confirm_entry> get_duplication_confirms_to_update() const;

    // advance the status of `dup` to `next_status`
    void update_duplication_status(dupid_t dupid, duplication_status::type next_status);

    // SEE: replica::on_checkpoint_timer()
    int64_t min_confirmed_decree() const;

    void remove_non_existed_duplications(const std::vector<duplication_entry> &);

    void remove_all_duplications()
    {
        // fast path
        if (_duplications.empty())
            return;

        _duplications.clear();
    }

    gpid get_gpid() { return _replica->get_gpid(); }

private:
    friend class duplication_test_base;
    friend class replica_stub_duplication_test;
    friend class replica_duplication_test;

    replica *_replica;

    // dupid -> duplication_entity
    std::map<dupid_t, mutation_duplicator_u_ptr> _duplications;
};

} // namespace replication
} // namespace dsn

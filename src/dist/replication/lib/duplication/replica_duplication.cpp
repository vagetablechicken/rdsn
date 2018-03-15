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

#include <boost/lexical_cast.hpp>
#include <dsn/dist/replication/duplication_common.h>

#include "replica_duplication.h"

namespace dsn {
namespace replication {

std::vector<duplication_confirm_entry>
replica::duplication_impl::get_duplication_confirms_to_update() const
{
    std::vector<duplication_confirm_entry> updates;
    for (auto &kv : _duplications) {
        mutation_duplicator_s_ptr duplicator = kv.second;
        duplication_view view = duplicator->view();
        if (view.last_decree != view.confirmed_decree) {
            duplication_confirm_entry entry;
            entry.dupid = duplicator->id();
            entry.confirmed_decree = view.last_decree;
            updates.emplace_back(entry);
        }
    }
    return updates;
}

void replica::duplication_impl::sync_duplication(const duplication_entry &ent)
{
    dassert(_replica->status() == partition_status::PS_PRIMARY, "");

    dupid_t dupid = ent.dupid;
    duplication_status::type next_status = ent.status;

    mutation_duplicator_s_ptr &dup = _duplications[dupid];
    if (dup == nullptr) {
        dup = std::make_shared<mutation_duplicator>(ent, _replica);
    } else {
        if (dup->view().status == next_status) {
            return;
        }
    }

    update_duplication_status(dupid, next_status);
}

void replica::duplication_impl::update_duplication_status(dupid_t dupid,
                                                          duplication_status::type next_status)
{
    ddebug_f("changing status of duplication(dupid: {}, gpid: {}) to {}",
             dupid,
             _replica->get_gpid(),
             duplication_status_to_string(next_status));

    mutation_duplicator *dup = _duplications[dupid].get();

    if (next_status == duplication_status::DS_START) {
        dup->start();
    } else if (next_status == duplication_status::DS_PAUSE) {
        dup->pause();
    } else {
        dassert("unexpected duplication status (%s)", duplication_status_to_string(next_status));
    }
}

void replica::duplication_impl::update_confirmed_points(
    const std::vector<duplication_confirm_entry> &confirmed_points)
{
    for (const duplication_confirm_entry &ce : confirmed_points) {
        auto it = _duplications.find(ce.dupid);
        if (it == _duplications.end()) {
            continue;
        }

        mutation_duplicator_s_ptr dup = it->second;

        duplication_view new_state = dup->view().set_confirmed_decree(ce.confirmed_decree);
        dup->update_state(new_state);
    }
}

int64_t replica::duplication_impl::min_confirmed_decree() const
{
    int64_t min_decree = std::numeric_limits<int64_t>::max();
    if (_replica->status() == partition_status::PS_PRIMARY) {
        for (auto &kv : _duplications) {
            const duplication_view &view = kv.second.get()->view();
            if (view.status == duplication_status::type::DS_REMOVED) {
                continue;
            }
            min_decree = std::min(min_decree, view.confirmed_decree);
        }
    }
    dassert(min_decree >= 0, "invalid min_decree %" PRId64, min_decree);
    return min_decree;
}

// Remove the duplications that are not in the `dup_list`.
void replica::duplication_impl::remove_non_existed_duplications(const
    std::vector<duplication_entry> &dup_list)
{
    std::set<dupid_t> new_set;
    std::set<dupid_t> remove_set;

    for (const auto &dup_ent : dup_list) {
        new_set.insert(dup_ent.dupid);
    }

    for (auto &pair : _duplications) {
        dupid_t dupid = pair.first;

        // if the duplication is not found in `dup_list`, remove it.
        if (new_set.find(dupid) == new_set.end()) {
            pair.second->pause();
            remove_set.insert(dupid);
        }
    }

    for (dupid_t dupid : remove_set) {
        _duplications.erase(dupid);
    }
}

} // namespace replication
} // namespace dsn

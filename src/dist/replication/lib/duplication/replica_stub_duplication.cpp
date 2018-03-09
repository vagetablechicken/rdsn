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

#include "dist/replication/lib/replica_stub.h"
#include "dist/replication/lib/replica.h"

#include "replica_stub_duplication.h"
#include "replica_duplication.h"

namespace dsn {
namespace replication {

void replica_stub::duplication_impl::duplication_sync()
{
    if (_stub->_state == NS_Disconnected) {
        // stop if is disconnected from meta server
        return;
    }

    ddebug("duplication_sync");

    auto req = make_unique<duplication_sync_request>();
    req->node = _stub->primary_address();
    {
        zauto_read_lock l(_stub->_replicas_lock);

        // collects confirm points from all replicas(primary) on this server
        for (auto &kv : _stub->_replicas) {
            const replica_ptr &replica = kv.second;
            const gpid &pid = kv.first;

            if (replica->status() != partition_status::PS_PRIMARY) {
                continue;
            }

            auto confirmed = replica->_duplication_impl->get_duplication_confirms_to_update();
            if (!confirmed.empty()) {
                req->confirm_list[pid] = std::move(confirmed);
            }
        }
    }
    call_duplication_sync_rpc(std::move(req));
}

void replica_stub::duplication_impl::on_duplication_sync_reply(error_code err,
                                                               duplication_sync_rpc rpc)
{
    ddebug("on_duplication_sync_reply");

    duplication_sync_response &resp = rpc.response();
    if (resp.err != ERR_OK) {
        err = resp.err;
    }
    if (err != ERR_OK) {
        dwarn("on_duplication_sync_reply: err(%s)", err.to_string());
    } else {
        update_duplication_map(resp.dup_map);

        if (!rpc.request().confirm_list.empty()) {
            update_confirmed_points(rpc.request().confirm_list);
        }
    }
}

// dup_map: <appid -> list<dup_entry>>
void replica_stub::duplication_impl::update_duplication_map(
    std::map<int32_t, std::vector<duplication_entry>> &dup_map)
{
    zauto_read_lock l(_stub->_replicas_lock);

    for (auto &ent : _stub->_replicas) {
        gpid pid = ent.first;
        replica_ptr r = ent.second;

        if (r->status() != partition_status::PS_PRIMARY) {
            continue;
        }

        std::vector<duplication_entry> dup_ent_list = std::move(dup_map[pid.get_app_id()]);
        for (const duplication_entry &dup_ent : dup_ent_list) {
            r->_duplication_impl->sync_duplication(dup_ent);
        }
        r->_duplication_impl->remove_non_existed_duplications(dup_ent_list);
    }
}

void replica_stub::duplication_impl::call_duplication_sync_rpc(
    std::unique_ptr<duplication_sync_request> req)
{
    duplication_sync_rpc rpc(std::move(req), RPC_CM_DUPLICATION_SYNC);
    rpc_address meta_server_address(_stub->get_meta_server_address());
    rpc.call(meta_server_address, _stub, [this, rpc](error_code err) {
        on_duplication_sync_reply(err, rpc);

        // start a new round of synchronization
        enqueue_duplication_sync_timer(
            std::chrono::milliseconds(_stub->_options.duplication_sync_interval_ms));
    });
}

void replica_stub::duplication_impl::update_confirmed_points(
    const std::map<gpid, std::vector<duplication_confirm_entry>> &confirmed_lists)
{
    for (auto &ent : confirmed_lists) {
        const gpid &pid = ent.first;

        auto it = _stub->_replicas.find(pid);
        if (it == _stub->_replicas.end()) {
            continue;
        }

        replica_ptr &r = it->second;
        r->_duplication_impl->update_confirmed_points(ent.second);
    }
}

} // namespace replication
} // namespace dsn

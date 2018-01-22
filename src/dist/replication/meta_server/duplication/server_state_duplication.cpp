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

#include <dsn/dist/error_code.h>
#include <dsn/dist/replication/duplication_common.h>
#include <dsn/dist/replication/fmt_logging.h>

#include "dist/replication/meta_server/meta_service.h"

#include "server_state_duplication.h"

namespace dsn {
namespace replication {

// DEVELOPER NOTES:
//
// Read operations for duplication are multi-threaded(THREAD_POOL_META_SERVER),
// but writes are always in a single-worker thread pool(THREAD_POOL_META_STATE).
// Therefore in each write-op, only write lock should be held when shared data
// changes. Holding read lock in THREAD_POOL_META_STATE is redundant.
// In read-op, please remember to hold read lock before accessing shared data.
//
// =============================================================================

inline duplication_entry construct_duplication_entry(const duplication_info &dup)
{
    duplication_entry entry;
    entry.dupid = dup.id;
    entry.create_ts = dup.create_timestamp_ms;
    entry.remote_address = dup.remote;
    entry.status = dup.get_status();
    return entry;
}

// Handle the request for duplication info of specific table.
// ThreadPool(READ): THREAD_POOL_META_SERVER
void server_state::duplication_impl::query_duplication_info(duplication_query_rpc &rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();

    ddebug("query duplication info for app:%s", request.app_name.c_str());

    response.err = ERR_OK;
    {
        zauto_read_lock l(app_lock());
        std::shared_ptr<app_state> app = _state->get_app(request.app_name);
        if (!app || app->status != app_status::AS_AVAILABLE) {
            response.err = ERR_APP_NOT_EXIST;
        } else {
            response.appid = app->app_id;
            for (auto &dup_id_to_info : app->duplications) {
                const duplication_info_s_ptr &dup = dup_id_to_info.second;
                response.entry_list.emplace_back(construct_duplication_entry(*dup));
            }
        }
    }
}

// Lock: no lock held.
// ThreadPool(WRITE): THREAD_POOL_META_STATE
void server_state::duplication_impl::do_duplication_status_change(
    std::shared_ptr<app_state> app, duplication_info_s_ptr dup, duplication_status_change_rpc &rpc)
{
    auto on_write_storage_complete = [this, rpc, app, dup](error_code error) {
        auto &resp = rpc.response();
        if (error == ERR_OK) {
            ddebug(
                "change duplication status on storage service successfully, app name: %s, appid: "
                "%" PRId32 " dupid:%" PRId32,
                app->app_name.c_str(),
                app->app_id,
                dup->id);

            dup->stable_status();
            resp.err = ERR_OK;
            resp.appid = app->app_id;
        } else if (error == ERR_OBJECT_NOT_FOUND) {
            derror("duplication(dupid: %d) is not found on meta storage", dup->id);
            resp.err = error;
        } else if (error == ERR_TIMEOUT) {
            dwarn("the storage service is not available currently, try again after 1 second");
            tasking::enqueue(LPC_META_STATE_HIGH,
                             tracker(),
                             std::bind(&duplication_impl::do_duplication_status_change,
                                       this,
                                       std::move(app),
                                       std::move(dup),
                                       rpc),
                             0,
                             std::chrono::seconds(1));
        } else {
            dassert(false, "we can't handle this error: %s", error.to_string());
        }
    };

    dup->alter_status(rpc.request().status);

    // store the duplication in requested status.
    blob value = dup->copy_in_status(rpc.request().status)->to_json_blob();

    std::string dup_path = get_duplication_path(*app, dup->id);
    _meta_svc->get_remote_storage()->set_data(
        dup->store_path, value, LPC_META_STATE_HIGH, on_write_storage_complete, tracker());
}

// ThreadPool(WRITE): THREAD_POOL_META_STATE
void server_state::duplication_impl::change_duplication_status(duplication_status_change_rpc rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();

    ddebug("change status of duplication(%d) to %s for app(%s)",
           request.dupid,
           duplication_status_to_string(request.status),
           request.app_name.c_str());

    dupid_t dupid = request.dupid;

    // must not handle the request if the app that the duplication binds to is unavailable
    std::shared_ptr<app_state> app = _state->get_app(request.app_name);
    if (!app || app->status != app_status::AS_AVAILABLE) {
        response.err = ERR_APP_NOT_EXIST;
        return;
    }

    duplication_info_s_ptr dup;
    auto kvp = app->duplications.find(dupid);
    if (kvp != app->duplications.end()) {
        dup = kvp->second;
    } else {
        response.err = ERR_OBJECT_NOT_FOUND;
        return;
    }

    response.err = dup->alter_status(request.status);
    if (response.err != ERR_OK) {
        return;
    }

    do_duplication_status_change(std::move(app), std::move(dup), rpc);
}

// Lock: no lock held.
// ThreadPool(WRITE): THREAD_POOL_META_STATE
void server_state::duplication_impl::do_add_duplication(std::shared_ptr<app_state> app,
                                                        duplication_info_s_ptr dup,
                                                        duplication_add_rpc rpc)
{
    auto on_write_storage_complete = [this, app, dup, rpc](error_code ec) {

        auto retry_do_add_duplication =
            std::bind(&duplication_impl::do_add_duplication, this, app, dup, rpc);

        auto &resp = rpc.response();
        if (ec == ERR_OK || ec == ERR_NODE_ALREADY_EXIST) {
            ddebug_f("add duplication on {} successfully, app name: {}, appid: {},"
                     " remote cluster address: {}, dupid: {}",
                     _meta_svc->get_meta_options().meta_state_service_type,
                     app->app_name,
                     app->app_id,
                     dup->remote,
                     dup->id);
            ddebug(dup->store_path.c_str());

            // The duplication starts only after it's been persisted.
            dup->stable_status();

            resp.err = ERR_OK;
            resp.appid = app->app_id;
            resp.dupid = dup->id;
        } else if (ec == ERR_TIMEOUT) {
            dwarn("request was timeout, retry after 1 second [do_add_duplication]");
            tasking::enqueue(LPC_META_STATE_HIGH,
                             tracker(),
                             retry_do_add_duplication,
                             0,
                             std::chrono::seconds(1));
        } else {
            dassert(false, "we can't handle this error(%s) [do_add_duplication]", ec.to_string());
        }
    };

    dup->start();

    // store the duplication in started state
    blob value = dup->copy_in_status(duplication_status::DS_START)->to_json_blob();

    _meta_svc->get_remote_storage()->create_node(
        dup->store_path, LPC_META_STATE_HIGH, on_write_storage_complete, value, tracker());
}

void server_state::duplication_impl::do_create_parent_dir_before_adding_duplication(
    std::shared_ptr<app_state> app, duplication_info_s_ptr dup, duplication_add_rpc rpc)
{
    std::string parent_path = get_duplication_path(*app);
    ddebug_f("do_create_parent_dir_before_adding_duplication (path: {})", parent_path);

    auto on_create_parent_complete = [this, app, dup, rpc](error_code ec) {
        auto retry_this = std::bind(
            &duplication_impl::do_create_parent_dir_before_adding_duplication, this, app, dup, rpc);

        if (ec == ERR_OK || ec == ERR_NODE_ALREADY_EXIST) {
            do_add_duplication(app, dup, rpc);
        } else if (ec == ERR_TIMEOUT) {
            dwarn("request was timeout, retry after 1 second "
                  "[do_create_parent_dir_before_adding_duplication]");
            tasking::enqueue(
                LPC_META_STATE_HIGH, tracker(), retry_this, 0, std::chrono::seconds(1));
        } else {
            dassert(
                false,
                "we can't handle this error(%s) [do_create_parent_dir_before_adding_duplication]",
                ec.to_string());
        }
    };

    _meta_svc->get_remote_storage()->create_node(
        parent_path, LPC_META_STATE_HIGH, on_create_parent_complete, blob(), tracker());
}

// ThreadPool(WRITE): THREAD_POOL_META_STATE
void server_state::duplication_impl::add_duplication(duplication_add_rpc rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();
    std::shared_ptr<app_state> app;

    ddebug("add duplication for app(%s), remote cluster address is %s",
           request.app_name.c_str(),
           request.remote_cluster_address.c_str());

    response.err = ERR_OK;
    //    if (dsn_uri_to_cluster_id(request.remote_cluster_address.c_str()) <= 0) {
    //        dwarn("invalid remote address(%s)", request.remote_cluster_address.c_str());
    //        response.err = ERR_INVALID_PARAMETERS;
    //    }

    if (response.err == ERR_OK) {
        app = _state->get_app(request.app_name);
        if (!app || app->status != app_status::AS_AVAILABLE) {
            response.err = ERR_APP_NOT_EXIST;
        }
        //        else if (app->envs["value_version"] != "1") {
        //            dwarn("unable to add duplication for %s since value_version(%s) is not \"1\"",
        //                  request.app_name.c_str(),
        //                  app->envs["value_version"].c_str());
        //            response.err = ERR_INVALID_VERSION;
        //        }
        else {
            duplication_info_s_ptr dup;
            for (auto &ent : app->duplications) {
                if (ent.second->remote == request.remote_cluster_address) {
                    dup = ent.second;
                }
            }

            if (dup) {
                ddebug_f("duplication(id: {}, remote: {}) for app({}) is added already",
                         dup->id,
                         dup->remote,
                         app->app_name);

                // don't create if existed.
                response.err = ERR_OK;
                response.dupid = dup->id;
                response.appid = app->app_id;
            } else {
                dup = new_dup_from_init(request.remote_cluster_address, app.get());
                do_create_parent_dir_before_adding_duplication(std::move(app), std::move(dup), rpc);
            }
        }
    }
}

// ThreadPool(WRITE): THREAD_POOL_META_STATE
void server_state::duplication_impl::duplication_sync(duplication_sync_rpc rpc)
{
    auto &request = rpc.request();
    auto &response = rpc.response();
    response.err = ERR_OK;

    node_state *ns = get_node_state(_state->_nodes, request.node, false);
    if (ns == nullptr) {
        dwarn("node(%s) not found in meta server", request.node.to_string());
        response.err = ERR_OBJECT_NOT_FOUND;
        return;
    }

    std::unordered_set<duplication_info_s_ptr> dup_to_update;
    for (const auto &kv : request.confirm_list) {
        do_update_progress_on_replica(*ns, kv.first, kv.second, &dup_to_update);
    }

    // upload the updated duplications to zookeeper.
    for (duplication_info_s_ptr dup : dup_to_update) {
        auto on_write_storage_complete = [dup](error_code ec) {
            if (ec == ERR_OK) {
                dup->stable_progress();
            } else {
                derror("error encountered (%s) while writing duplication(%d) to meta storage "
                       "[duplication_sync]",
                       ec.to_string(),
                       dup->id);
            }
        };

        blob value = dsn::json::json_forwarder<duplication_info>::encode(*dup);
        _meta_svc->get_remote_storage()->set_data(
            dup->store_path, value, LPC_META_STATE_HIGH, on_write_storage_complete, tracker());
    }

    do_get_dup_map_on_replica(*ns, &response.dup_map);
}

// Locks: no lock held
// ThreadPool(WRITE): THREAD_POOL_META_STATE
void server_state::duplication_impl::do_update_progress_on_replica(
    const node_state &ns,
    const dsn::gpid &gpid,
    const std::vector<duplication_confirm_entry> &confirm_points,
    std::unordered_set<duplication_info_s_ptr> *dup_to_update)
{
    if (ns.served_as(gpid) != partition_status::PS_PRIMARY) {
        // ignore if this partition is not primary.
        return;
    }

    int app_id = gpid.get_app_id();
    int pid = gpid.get_partition_index();
    std::shared_ptr<app_state> app = _state->get_app(app_id);
    if (!app || app->status != app_status::AS_AVAILABLE) {
        // ignore if this partition is unavailable
        return;
    }

    // for each dup on this replica
    for (const duplication_confirm_entry &confirm : confirm_points) {
        auto kvp = app->duplications.find(confirm.dupid);
        if (kvp == app->duplications.end()) {
            // the requested dup is not found on meta
            continue;
        }

        duplication_info_s_ptr &dup = kvp->second;
        if (dup->alter_progress(pid, confirm.confirmed_decree)) {
            dup_to_update->insert(dup);
        }
    }
}

// NOTE: dup_map never includes those apps that don't have a duplication.
void server_state::duplication_impl::do_get_dup_map_on_replica(
    const node_state &ns, std::map<int32_t, std::vector<duplication_entry>> *dup_map) const
{
    ns.for_each_partition([this, &dup_map, &ns](const gpid &pid) -> bool {
        if (ns.served_as(pid) != partition_status::PS_PRIMARY) {
            return true;
        }

        if (dup_map->find(pid.get_app_id()) != dup_map->end()) {
            return true;
        }

        std::shared_ptr<app_state> app = _state->get_app(pid.get_app_id());
        dassert(app != nullptr, "");
        if (app->duplications.empty()) {
            return true;
        }

        auto &dup_list_for_app = (*dup_map)[pid.get_app_id()];
        for (auto &kv : app->duplications) {
            duplication_info_s_ptr &dup = kv.second;

            if (dup->status == duplication_status::DS_START ||
                dup->status == duplication_status::DS_PAUSE) {
                dup_list_for_app.emplace_back(construct_duplication_entry(*dup));
            }
        }
        return true;
    });
}

server_state::duplication_impl::duplication_impl(server_state *state, meta_service *meta)
    : _state(state), _meta_svc(meta), _tracker(1)
{
    dassert(_state, "duplication_impl::duplication_impl: _state should not be null");
    dassert(_meta_svc, "duplication_impl::duplication_impl: _meta_svc should not be null");
}

std::shared_ptr<duplication_info>
server_state::duplication_impl::new_dup_from_init(const std::string &remote_cluster_address,
                                                  app_state *app) const
{
    duplication_info_s_ptr dup;

    // use current time to identify this duplication.
    auto dupid = static_cast<dupid_t>(dsn_now_ms() / 1000);
    {
        zauto_write_lock(app_lock());

        // hold write lock here to ensure that dupid is unique
        while (app->duplications.find(dupid) != app->duplications.end())
            dupid++;
        dup = std::make_shared<duplication_info>(
            dupid, remote_cluster_address, get_duplication_path(*app, dupid));
        app->duplications.emplace(dup->id, dup);
    }

    return dup;
}

// ThreadPool(WRITE): THREAD_POOL_META_STATE
void server_state::duplication_impl::recover_from_meta_state()
{
    for (const auto &kv : _state->_exist_apps) {
        auto app = kv.second;

        do_recover_from_meta_state_for_app(app);
    }
}

// ThreadPool(WRITE): THREAD_POOL_META_STATE
void server_state::duplication_impl::do_recover_from_meta_state_for_app(
    std::shared_ptr<app_state> app)
{
    _meta_svc->get_remote_storage()->get_children(
        get_duplication_path(*app),
        LPC_META_STATE_HIGH,
        [app, this](error_code ec, const std::vector<std::string> &dup_id_list) {
            if (app->status != app_status::AS_AVAILABLE) {
                return;
            }

            if (ec == ERR_OK || ec == ERR_OBJECT_NOT_FOUND) {
                // ERR_OBJECT_NOT_FOUND means the app has no dup assigned.

                for (const std::string &raw_dup_id : dup_id_list) {
                    do_restore_dup_from_meta_state(raw_dup_id, app);
                }
            } else if (ec == ERR_TIMEOUT) {
                auto retry_this = std::bind(
                    &duplication_impl::do_recover_from_meta_state_for_app, this, std::move(app));

                derror("request was timeout, retry again after 1 second "
                       "[do_recover_from_meta_state_for_app]");
                tasking::enqueue(
                    LPC_META_STATE_HIGH, tracker(), retry_this, 0, std::chrono::seconds(1));
            } else {
                derror("error encountered (%s) while recovering duplications of app(%s) "
                       "from meta storage(%s) [do_recover_from_meta_state_for_app]",
                       ec.to_string(),
                       app->app_name.c_str(),
                       get_duplication_path(*app).c_str());
            }
        },
        tracker());
}

void server_state::duplication_impl::do_restore_dup_from_meta_state(const std::string &dupid,
                                                                    std::shared_ptr<app_state> app)
{
    _meta_svc->get_remote_storage()->get_data(
        get_duplication_path(*app, dupid),
        LPC_META_STATE_HIGH,
        [app, dupid, this](error_code ec, const blob &blob_dup_info) {
            if (ec == ERR_OK) {
                auto dup = duplication_info::create_from_blob(blob_dup_info);
                app->duplications[dup->id] = dup;
            } else if (ec == ERR_TIMEOUT) {
                auto retry_this =
                    std::bind(&duplication_impl::do_restore_dup_from_meta_state, this, dupid, app);

                derror("request was timeout, retry again after 1 second "
                       "[do_restore_dup_from_meta_state]");
                tasking::enqueue(
                    LPC_META_STATE_HIGH, tracker(), retry_this, 0, std::chrono::seconds(1));
            } else {
                derror("error encountered (%s) when restoring duplication "
                       "from meta storage(%s) [do_restore_dup_from_meta_state]",
                       ec.to_string(),
                       app->app_name.c_str(),
                       get_duplication_path(*app).c_str());
            }
        },
        tracker());
}

// SEE: meta_service::on_query_duplication_info
void server_state::query_duplication_info(duplication_query_rpc rpc)
{
    dassert(_duplication_impl, "duplication_impl is uninitialized");
    _duplication_impl->query_duplication_info(rpc);
}

// SEE: meta_service::on_add_duplication
void server_state::add_duplication(duplication_add_rpc rpc)
{
    dassert(_duplication_impl, "duplication_impl is uninitialized");
    _duplication_impl->add_duplication(std::move(rpc));
}

// SEE: meta_service::on_change_duplication_status
void server_state::change_duplication_status(duplication_status_change_rpc rpc)
{
    dassert(_duplication_impl, "duplication_impl is uninitialized");
    _duplication_impl->change_duplication_status(std::move(rpc));
}

// SEE: meta_service::on_duplication_sync
void server_state::duplication_sync(duplication_sync_rpc rpc)
{
    dassert(_duplication_impl, "duplication_impl is uninitialized");
    _duplication_impl->duplication_sync(std::move(rpc));
}

void server_state::recover_from_meta_state()
{
    dassert(_duplication_impl, "duplication_impl is uninitialized");
    _duplication_impl->recover_from_meta_state();
}

} // namespace replication
} // namespace dsn

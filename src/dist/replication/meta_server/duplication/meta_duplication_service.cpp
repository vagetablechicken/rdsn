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

#include <dsn/dist/replication/duplication_common.h>
#include <dsn/utility/chrono_literals.h>

#include "dist/replication/meta_server/meta_service.h"

#include "meta_duplication_service.h"

namespace dsn {
namespace replication {

using namespace dsn::literals::chrono_literals;

// DEVELOPER NOTES:
//
// Read operations for duplication are multi-threaded(THREAD_POOL_META_SERVER),
// but writes are always in a single-worker thread pool(THREAD_POOL_META_STATE).
// Therefore in each write-op, only write lock should be held when shared data
// changes. Holding read lock in THREAD_POOL_META_STATE is redundant.
// In read-op, please remember to hold read lock before accessing shared data.
//
// =============================================================================

// NOT thread-safe.
// REQUIRE read lock on `dup` and `ns` == nullptr, or
// REQUIRE task running in THREAD_POOL_META_STATE.
// \see meta_duplication_service::query_duplication_info
// \see meta_duplication_service::do_get_dup_map_on_replica
inline duplication_entry construct_duplication_entry(const duplication_info &dup,
                                                     const node_state *ns = nullptr)
{
    duplication_entry entry;
    entry.dupid = dup.id;
    entry.create_ts = dup.create_timestamp_ms;
    entry.remote_address = dup.remote;
    entry.status = dup.status;

    if (ns != nullptr) {
        // reduce the number of partitions piggybacked in `entry.progress`
        ns->for_each_primary(dup.app_id, [&entry, &dup](const gpid &pid) -> bool {
            auto it = dup.progress.find(pid.get_partition_index());
            if (it != dup.progress.end()) {
                entry.progress[pid.get_partition_index()] = it->second;
            }
            return true;
        });
    } else {
        entry.progress = dup.progress;
    }

    return entry;
}

// Handle the request for duplication info of specific table.
// ThreadPool(READ): THREAD_POOL_META_SERVER
void meta_duplication_service::query_duplication_info(duplication_query_rpc &rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();

    ddebug_f("query duplication info for app: {}", request.app_name);

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
                {
                    zauto_read_lock dup_lock(dup->lock_unsafe());

                    // the removed duplication is not visible to user.
                    if (dup->status != duplication_status::DS_REMOVED) {
                        response.entry_list.emplace_back(construct_duplication_entry(*dup));
                    }
                }
            }
        }
    }
}

// ThreadPool(WRITE): THREAD_POOL_META_STATE
void meta_duplication_service::do_duplication_status_change(std::shared_ptr<app_state> app,
                                                            duplication_info_s_ptr dup,
                                                            duplication_status_change_rpc &rpc)
{
    auto on_write_storage_complete = [this, rpc, app, dup](error_code error) {
        auto &resp = rpc.response();
        if (error == ERR_OK) {
            ddebug_f("change duplication status on storage service successfully, app name: {}, "
                     "appid: {} dupid: {}",
                     app->app_name,
                     app->app_id,
                     dup->id);

            dup->stable_status();
            resp.err = ERR_OK;
            resp.appid = app->app_id;
        } else if (error == ERR_OBJECT_NOT_FOUND) {
            derror_f("duplication(dupid: {}) is not found on meta storage", dup->id);
            resp.err = error;
        } else if (error == ERR_TIMEOUT) {
            dwarn("meta storage is not available currently, try again after 1 second");
            tasking::enqueue(LPC_META_STATE_HIGH,
                             tracker(),
                             std::bind(&meta_duplication_service::do_duplication_status_change,
                                       this,
                                       std::move(app),
                                       std::move(dup),
                                       rpc),
                             0,
                             1_s);
        } else {
            dfatal_f("we can't handle this error: {}", error);
        }
    };

    // store the duplication in requested status.
    blob value = dup->copy_in_status(rpc.request().status)->to_json_blob();

    _meta_svc->get_remote_storage()->set_data(
        dup->store_path, value, LPC_META_STATE_HIGH, on_write_storage_complete, tracker());
}

// ThreadPool(WRITE): THREAD_POOL_META_STATE
void meta_duplication_service::change_duplication_status(duplication_status_change_rpc rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();

    ddebug_f("change status of duplication({}) to {} for app({})",
             request.dupid,
             duplication_status_to_string(request.status),
             request.app_name);

    dupid_t dupid = request.dupid;

    std::shared_ptr<app_state> app = _state->get_app(request.app_name);
    if (!app || app->status != app_status::AS_AVAILABLE) {
        response.err = ERR_APP_NOT_EXIST;
        return;
    }

    duplication_info_s_ptr dup = app->duplications[dupid];
    if (dup == nullptr) {
        response.err = ERR_OBJECT_NOT_FOUND;
        return;
    }

    response.err = dup->alter_status(request.status);
    if (response.err != ERR_OK) {
        return;
    }

    do_duplication_status_change(std::move(app), std::move(dup), rpc);
}

// ThreadPool(WRITE): THREAD_POOL_META_STATE
void meta_duplication_service::do_add_duplication(std::shared_ptr<app_state> app,
                                                  duplication_info_s_ptr dup,
                                                  duplication_add_rpc rpc)
{
    ddebug_f("create node({}) for duplication", get_duplication_path(*app, dup->id));

    auto on_write_storage_complete = [this, app, dup, rpc](error_code ec) {

        auto retry_do_add_duplication =
            std::bind(&meta_duplication_service::do_add_duplication, this, app, dup, rpc);

        auto &resp = rpc.response();
        if (ec == ERR_OK || ec == ERR_NODE_ALREADY_EXIST) {
            ddebug_f("add duplication successfully, app name: {}, appid: {},"
                     " remote cluster address: {}, dupid: {}",
                     app->app_name,
                     app->app_id,
                     dup->remote,
                     dup->id);

            // The duplication starts only after it's been persisted.
            dup->stable_status();

            resp.err = ERR_OK;
            resp.appid = app->app_id;
            resp.dupid = dup->id;
        } else if (ec == ERR_TIMEOUT) {
            dwarn("request was timeout, retry after 1 second");
            tasking::enqueue(LPC_META_STATE_HIGH, tracker(), retry_do_add_duplication, 0, 1_s);
        } else {
            dfatal_f("we can't handle this error({})", ec);
        }
    };

    dup->start();

    // store the duplication in started state
    blob value = dup->copy_in_status(duplication_status::DS_START)->to_json_blob();

    _meta_svc->get_remote_storage()->create_node(
        dup->store_path, LPC_META_STATE_HIGH, on_write_storage_complete, value, tracker());
}

void meta_duplication_service::do_create_parent_dir_before_adding_duplication(
    std::shared_ptr<app_state> app, duplication_info_s_ptr dup, duplication_add_rpc rpc)
{
    std::string parent_path = get_duplication_path(*app);
    ddebug_f("create parent directory({}) for duplication({})", parent_path, dup->id);

    auto on_create_parent_complete = [this, app, dup, rpc](error_code ec) {
        auto retry_this =
            std::bind(&meta_duplication_service::do_create_parent_dir_before_adding_duplication,
                      this,
                      app,
                      dup,
                      rpc);

        if (ec == ERR_OK || ec == ERR_NODE_ALREADY_EXIST) {
            do_add_duplication(app, dup, rpc);
        } else if (ec == ERR_TIMEOUT) {
            dwarn("request was timeout, retry after 1 second");
            tasking::enqueue(LPC_META_STATE_HIGH, tracker(), retry_this, 0, 1_s);
        } else {
            dfatal_f("we can't handle this error({})", ec);
        }
    };

    _meta_svc->get_remote_storage()->create_node(
        parent_path, LPC_META_STATE_HIGH, on_create_parent_complete, blob(), tracker());
}

// Note that the rpc will not create a new one if the duplication
// with the same app and remote end point already exists.
// ThreadPool(WRITE): THREAD_POOL_META_STATE
void meta_duplication_service::add_duplication(duplication_add_rpc rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();
    std::shared_ptr<app_state> app;

    ddebug_f("add duplication for app({}), remote cluster address is {}",
             request.app_name,
             request.remote_cluster_address);

    response.err = ERR_OK;
    //    if (dsn_uri_to_cluster_id(request.remote_cluster_address.c_str()) <= 0) {
    //        dwarn("invalid remote address(%s)", request.remote_cluster_address.c_str());
    //        response.err = ERR_INVALID_PARAMETERS;
    //        return;
    //    }
    app = _state->get_app(request.app_name);
    if (!app || app->status != app_status::AS_AVAILABLE) {
        response.err = ERR_APP_NOT_EXIST;
        return;
    }
    //        if (app->envs["value_version"] != "1") {
    //            dwarn("unable to add duplication for %s since value_version(%s) is not \"1\"",
    //                  request.app_name.c_str(),
    //                  app->envs["value_version"].c_str());
    //            response.err = ERR_INVALID_VERSION;
    //            return;
    //        }
    duplication_info_s_ptr dup;
    for (auto &ent : app->duplications) {
        auto it = ent.second;
        if (it->remote == request.remote_cluster_address) {
            dup = ent.second;
            break;
        }
    }
    if (!dup) {
        dup = new_dup_from_init(request.remote_cluster_address, app.get());
    }

    do_create_parent_dir_before_adding_duplication(std::move(app), std::move(dup), rpc);
}

// ThreadPool(WRITE): THREAD_POOL_META_STATE
void meta_duplication_service::duplication_sync(duplication_sync_rpc rpc)
{
    auto &request = rpc.request();
    auto &response = rpc.response();
    response.err = ERR_OK;

    node_state *ns = get_node_state(_state->_nodes, request.node, false);
    if (ns == nullptr) {
        dwarn_f("node({}) is not found in meta server", request.node.to_string());
        response.err = ERR_OBJECT_NOT_FOUND;
        return;
    }

    std::unordered_set<duplication_info_s_ptr> dup_to_update;
    for (const auto &kv : request.confirm_list) {
        do_update_progress_on_replica(*ns, kv.first, kv.second, &dup_to_update);
    }

    // upload the updated duplications to zookeeper.
    for (const duplication_info_s_ptr &dup : dup_to_update) {
        auto on_write_storage_complete = [dup](error_code ec) {
            if (ec == ERR_OK) {
                dup->stable_progress();
            } else {
                derror_f("error encountered ({}) while writing duplication({}) to meta storage",
                         ec,
                         dup->id);
            }
        };

        blob value = dsn::json::json_forwarder<duplication_info>::encode(*dup);
        _meta_svc->get_remote_storage()->set_data(
            dup->store_path, value, LPC_META_STATE_HIGH, on_write_storage_complete, tracker());
    }

    // respond immediately before state persisted.
    do_get_dup_map_on_replica(*ns, &response.dup_map);
}

// ThreadPool(WRITE): THREAD_POOL_META_STATE
void meta_duplication_service::do_update_progress_on_replica(
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
// ThreadPool(WRITE): THREAD_POOL_META_STATE
// dup_map = map<appid, list<dup_entry>>
void meta_duplication_service::do_get_dup_map_on_replica(
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
        dassert(app != nullptr, "server_state is inconsistent with node_state");
        if (app->duplications.empty()) {
            return true;
        }

        /// ==== for each app: having primary on this node && having duplication === ///

        auto &dup_list_for_app = (*dup_map)[pid.get_app_id()];
        for (auto &kv : app->duplications) {
            duplication_info_s_ptr &dup = kv.second;
            if (dup->status == duplication_status::DS_START ||
                dup->status == duplication_status::DS_PAUSE) {
                dup_list_for_app.emplace_back(construct_duplication_entry(*dup, &ns));
            }
        }
        return true;
    });
}

meta_duplication_service::meta_duplication_service(server_state *state, meta_service *meta)
    : _state(state), _meta_svc(meta), _tracker(1)
{
    dassert(_state, "_state should not be null");
    dassert(_meta_svc, "_meta_svc should not be null");
}

std::shared_ptr<duplication_info>
meta_duplication_service::new_dup_from_init(const std::string &remote_cluster_address,
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
            dupid, app->app_id, remote_cluster_address, get_duplication_path(*app, dupid));
        app->duplications.emplace(dup->id, dup);
    }

    return dup;
}

// ThreadPool(WRITE): THREAD_POOL_META_STATE
void meta_duplication_service::recover_from_meta_state()
{
    for (const auto &kv : _state->_exist_apps) {
        auto app = kv.second;

        do_recover_from_meta_state_for_app(app);
    }
}

// ThreadPool(WRITE): THREAD_POOL_META_STATE
void meta_duplication_service::do_recover_from_meta_state_for_app(std::shared_ptr<app_state> app)
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
                auto retry_this =
                    std::bind(&meta_duplication_service::do_recover_from_meta_state_for_app,
                              this,
                              std::move(app));

                derror("request was timeout, retry again after 1 second");
                tasking::enqueue(LPC_META_STATE_HIGH, tracker(), retry_this, 0, 1_s);
            } else {
                derror_f("error encountered ({}) while recovering duplications of app({}) "
                         "from meta storage({})",
                         ec,
                         app->app_name,
                         get_duplication_path(*app));
            }
        },
        tracker());
}

void meta_duplication_service::do_restore_dup_from_meta_state(const std::string &dupid,
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
                auto retry_this = std::bind(
                    &meta_duplication_service::do_restore_dup_from_meta_state, this, dupid, app);

                derror("request was timeout, retry again after 1 second");
                tasking::enqueue(LPC_META_STATE_HIGH, tracker(), retry_this, 0, 1_s);
            } else {
                derror_f("error encountered ({}) when restoring duplication [app({}) dupid({})] "
                         "from meta storage({})",
                         ec,
                         app->app_name,
                         dupid,
                         get_duplication_path(*app));
            }
        },
        tracker());
}

} // namespace replication
} // namespace dsn

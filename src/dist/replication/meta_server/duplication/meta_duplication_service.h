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

#include "dist/replication/meta_server/server_state.h"
#include "dist/replication/meta_server/meta_data.h"

namespace dsn {
namespace replication {

class meta_duplication_service
{
public:
    meta_duplication_service(server_state *ss, meta_service *ms);

    void query_duplication_info(duplication_query_rpc &rpc);

    void add_duplication(duplication_add_rpc rpc);

    void change_duplication_status(duplication_status_change_rpc rpc);

    void duplication_sync(duplication_sync_rpc rpc);

    // Create a new duplication from INIT state.
    // Thread-Safe
    std::shared_ptr<duplication_info> new_dup_from_init(const std::string &remote_cluster_address,
                                                        app_state *app) const;

    // Recover from meta state storage.
    void recover_from_meta_state();

    /// ================================= Implementation =================================== ///

    void do_recover_from_meta_state_for_app(std::shared_ptr<app_state> app);

    void do_restore_dup_from_meta_state(const std::string &dupid, std::shared_ptr<app_state> app);

    void do_add_duplication(std::shared_ptr<app_state> app,
                            duplication_info_s_ptr dup,
                            duplication_add_rpc rpc);

    void do_create_parent_dir_before_adding_duplication(std::shared_ptr<app_state> app,
                                                        duplication_info_s_ptr dup,
                                                        duplication_add_rpc rpc);

    void do_duplication_status_change(std::shared_ptr<app_state> app,
                                      duplication_info_s_ptr dup,
                                      duplication_status_change_rpc &rpc);

    // update progress if they are advanced by the given confirm point.
    // the duplications that are updated will be included in `dup_to_update`.
    void do_update_progress_on_replica(const node_state &ns,
                                       const dsn::gpid &pid,
                                       const std::vector<duplication_confirm_entry> &confirm_points,
                                       std::unordered_set<duplication_info_s_ptr> *dup_to_update);

    void
    do_get_dup_map_on_replica(const node_state &ns,
                              std::map<int32_t, std::vector<duplication_entry>> *dup_map) const;

    // Get zk path for duplication.
    // Each app has a subdirectory containing the full list of dups that it holds, each of which is
    // a node whose key is a dupid and value is a json-serialized duplication_info.
    std::string get_duplication_path(const app_state &app, dupid_t dupid = -1) const
    {
        if (dupid < 0) {
            return _state->get_app_path(app) + "/duplication";
        }
        return _state->get_app_path(app) + "/duplication/" + std::to_string(dupid);
    }
    std::string get_duplication_path(const app_state &app, const std::string &dupid) const
    {
        return _state->get_app_path(app) + "/duplication/" + dupid;
    }

    // get lock to protect access of app table
    zrwlock_nr &app_lock() const { return _state->_lock; }

    clientlet *tracker() { return &_tracker; }
    void wait_all() { dsn_task_tracker_wait_all(tracker()->tracker()); }

private:
    friend class meta_duplication_service_test;

    server_state *_state;

    meta_service *_meta_svc;

    clientlet _tracker;
};

} // namespace replication
} // namespace dsn

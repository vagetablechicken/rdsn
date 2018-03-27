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

#include <dsn/dist/replication/replication_app_base.h>
#include <dsn/dist/replication/duplication_backlog_handler.h>

namespace dsn {
namespace replication {

class replication_app_base_for_duplication : public replication_app_base
{
public:
    explicit replication_app_base_for_duplication(replica *replica) : replication_app_base(replica)
    {
    }

    ::dsn::error_code start(int argc, char **argv) override { return ERR_NOT_IMPLEMENTED; }

    ::dsn::error_code stop(bool clear_state) override { return ERR_NOT_IMPLEMENTED; }

    ::dsn::error_code sync_checkpoint() override { return ERR_NOT_IMPLEMENTED; }

    ::dsn::error_code async_checkpoint(bool is_emergency) override { return ERR_NOT_IMPLEMENTED; }

    ::dsn::error_code prepare_get_checkpoint(/*out*/ ::dsn::blob &learn_req) override
    {
        return ERR_NOT_IMPLEMENTED;
    }

    ::dsn::error_code get_checkpoint(int64_t learn_start,
                                     const ::dsn::blob &learn_request,
                                     /*out*/ learn_state &state) override
    {
        return ERR_NOT_IMPLEMENTED;
    }

    ::dsn::error_code storage_apply_checkpoint(chkpt_apply_mode mode,
                                               const learn_state &state) override
    {
        return ERR_NOT_IMPLEMENTED;
    }

    ::dsn::error_code copy_checkpoint_to_dir(const char *checkpoint_dir,
                                             /*output*/ int64_t *last_decree) override
    {
        return ERR_NOT_IMPLEMENTED;
    }

    ::dsn::replication::decree last_durable_decree() const override { return 0; }

    int on_request(dsn_message_t request) override { return 0; }
};

class mock_replica : public replica
{
public:
    mock_replica(replica_stub *stub, gpid gpid, const app_info &app, const char *dir)
        : replica(stub, gpid, app, dir, false)
    {
        _app = dsn::make_unique<dsn::replication::replication_app_base_for_duplication>(this);
    }

    ~mock_replica() override
    {
        _config.status = partition_status::PS_INACTIVE;
        _app.reset(nullptr);
    }

    void init_private_log(mutation_log_ptr log) { _private_log = log; }

    replica::duplication_impl &get_replica_duplication_impl() { return *_duplication_impl; }

    void as_primary() { _config.status = partition_status::PS_PRIMARY; }
};

inline std::unique_ptr<mock_replica> create_mock_replica(replica_stub *stub,
                                                         int appid = 1,
                                                         int partition_index = 1,
                                                         const char *dir = "./")
{
    gpid gpid(appid, partition_index);
    app_info app_info;
    app_info.app_type = "replica";

    return make_unique<mock_replica>(stub, gpid, app_info, dir);
}

class mock_replica_stub : public replica_stub
{
public:
    mock_replica_stub() = default;

    ~mock_replica_stub() override = default;

    mock_replica_stub::duplication_impl &get_replica_stub_duplication_impl()
    {
        return *_duplication_impl;
    }

    void add_replica(replica *r) { _replicas[r->get_gpid()] = replica_ptr(r); }

    mock_replica *add_primary_replica(int appid, int part_index = 1)
    {
        auto r = add_non_primary_replica(appid, part_index);
        r->as_primary();
        return r;
    }

    mock_replica *add_non_primary_replica(int appid, int part_index = 1)
    {
        auto r = create_mock_replica(this, appid, part_index).release();
        add_replica(r);
        mock_replicas[gpid(appid, part_index)] = r;
        return r;
    }

    mock_replica *find_replica(int appid, int part_index = 1)
    {
        return mock_replicas[gpid(appid, part_index)];
    }

    void set_state_connected() { _state = replica_node_state::NS_Connected; }

    rpc_address get_meta_server_address() const override { return rpc_address("127.0.0.2", 12321); }

    std::map<gpid, mock_replica *> mock_replicas;
};

struct mock_duplication_backlog_handler : public duplication_backlog_handler
{
    explicit mock_duplication_backlog_handler(gpid id) : duplication_backlog_handler(id) {}

    // thread-safe
    void duplicate(mutation_tuple mut, err_callback cb) override
    {
        std::lock_guard<std::mutex> _(lock);
        auto err = dsn::error_s::ok();
        if (_err_hook) {
            err = _err_hook();
        }
        if (err.is_ok()) {
            mutation_list.emplace_back(std::get<2>(mut).to_string());
        }
        cb(err);
    }

    // thread-safe
    std::vector<std::string> get_mutation_list_safe()
    {
        std::lock_guard<std::mutex> _(lock);
        return mutation_list;
    }

    typedef std::function<dsn::error_s()> error_hook;
    void set_error_hook(error_hook hook) { _err_hook = std::move(hook); }

    std::vector<std::string> mutation_list;
    mutable std::mutex lock;
    error_hook _err_hook;
};

struct mock_duplication_backlog_handler_factory : public duplication_backlog_handler_factory
{
    std::unique_ptr<duplication_backlog_handler>
    create(gpid id, const std::string &remote_cluster_address, const std::string &app) override
    {
        return dsn::make_unique<mock_duplication_backlog_handler>(id);
    }
};

} // namespace replication
} // namespace dsn

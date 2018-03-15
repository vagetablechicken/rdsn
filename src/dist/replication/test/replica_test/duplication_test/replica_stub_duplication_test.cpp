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

#include "dist/replication/lib/duplication/replica_stub_duplication.h"

#include "duplication_test_base.h"

namespace dsn {
namespace replication {

struct replica_stub_duplication_test : public duplication_test_base
{
    void SetUp() override
    {
        stub = dsn::make_unique<mock_replica_stub>();
        dup_impl = &stub->get_replica_stub_duplication_impl();
    }

    void TearDown() override { stub.reset(); }

    void test_on_duplication_sync_reply()
    {
        // replica: {app_id:2, partition_id:1, duplications:{}}
        stub->add_primary_replica(2, 1);
        ASSERT_NE(stub->find_replica(2, 1), nullptr);

        auto req = dsn::make_unique<duplication_sync_request>();
        duplication_sync_rpc rpc(std::move(req), RPC_CM_DUPLICATION_SYNC);

        // appid:2 -> dupid:1
        duplication_entry ent;
        ent.dupid = 1;
        ent.remote_address = "dsn://slave-cluster";
        ent.status = duplication_status::DS_PAUSE;
        ent.confirmed_decree = 0;
        rpc.response().dup_map[2] = {ent};

        dup_impl->on_duplication_sync_reply(dsn::ERR_OK, rpc);
        mutation_duplicator_s_ptr dup =
            stub->find_replica(2, 1)->get_replica_duplication_impl()._duplications[1];

        ASSERT_TRUE(dup);
        ASSERT_EQ(dup->view().status, duplication_status::DS_PAUSE);
    }

protected:
    std::unique_ptr<mock_replica_stub> stub;
    mock_replica_stub::duplication_impl *dup_impl;
};

} // namespace replication
} // namespace dsn

using namespace dsn::replication;

TEST_F(replica_stub_duplication_test, duplication_sync)
{
    int total_app_num = 4;
    for (int appid = 1; appid <= total_app_num; appid++) {
        auto r = stub->add_non_primary_replica(appid, 1);

        duplication_entry ent;
        ent.dupid = 1;
        auto dup = std::make_shared<mutation_duplicator>(ent, r);
        dup->update_state(dup->view().set_last_decree(1).set_confirmed_decree(2));
        add_dup(r, std::move(dup));
    }

    RPC_MOCKING(duplication_sync_rpc)
    {
        {
            // replica server should not sync to meta when it's disconnected
            dup_impl->duplication_sync();
            ASSERT_EQ(duplication_sync_rpc::mail_box().size(), 0);
        }
        {
            // never collects confirm points from non-primaries
            stub->set_state_connected();
            dup_impl->duplication_sync();
            ASSERT_EQ(duplication_sync_rpc::mail_box().size(), 1);

            duplication_sync_request &req = duplication_sync_rpc::mail_box().back();
            ASSERT_EQ(req.confirm_list.size(), 0);
        }
    }

    RPC_MOCKING(duplication_sync_rpc)
    {
        for (auto &e : stub->mock_replicas) {
            e.second->as_primary();
        }
        dup_impl->duplication_sync();
        ASSERT_EQ(duplication_sync_rpc::mail_box().size(), 1);

        duplication_sync_request &req = duplication_sync_rpc::mail_box().back();
        ASSERT_EQ(req.node, stub->primary_address());
        ASSERT_EQ(req.confirm_list.size(), total_app_num);

        for (int appid = 1; appid <= total_app_num; appid++) {
            ASSERT_TRUE(req.confirm_list.find(dsn::gpid(appid, 1)) != req.confirm_list.end());
        }
    }
}

TEST_F(replica_stub_duplication_test, update_duplication_map)
{
    std::map<int32_t, std::vector<duplication_entry>> dup_map;
    for (int32_t appid = 1; appid <= 10; appid++) {
        for (int partition_id = 0; partition_id < 3; partition_id++) {
            stub->add_primary_replica(appid, partition_id);
        }
    }

    { // Ensure update_duplication_map adds new duplications if they are not existed.
        duplication_entry ent;
        ent.dupid = 2;
        ent.status = duplication_status::DS_PAUSE;
        ent.confirmed_decree = 0;

        // add duplication 2 for app 1, 3, 5 (of course in real world cases duplications
        // will not be the same for different tables)
        dup_map[1].push_back(ent);
        dup_map[3].push_back(ent);
        dup_map[5].push_back(ent);

        dup_impl->update_duplication_map(dup_map);

        for (int32_t appid : {1, 3, 5}) {
            for (int partition_id : {0, 1, 2}) {
                auto dup = find_dup(stub->find_replica(appid, partition_id), 2);
                ASSERT_TRUE(dup);
            }
        }

        // update duplicated decree of 1, 3, 5 to 2
        auto dup = find_dup(stub->find_replica(1, 1), 2);
        dup->update_state(dup->view().set_last_decree(2));

        dup = find_dup(stub->find_replica(3, 1), 2);
        dup->update_state(dup->view().set_last_decree(2));

        dup = find_dup(stub->find_replica(5, 1), 2);
        dup->update_state(dup->view().set_last_decree(2));
    }

    RPC_MOCKING(duplication_sync_rpc)
    {
        stub->set_state_connected();
        dup_impl->duplication_sync();
        ASSERT_EQ(duplication_sync_rpc::mail_box().size(), 1);

        auto &req = duplication_sync_rpc::mail_box().back();
        ASSERT_EQ(req.confirm_list.size(), 3);

        ASSERT_TRUE(req.confirm_list.find(dsn::gpid(1, 1)) != req.confirm_list.end());
        ASSERT_TRUE(req.confirm_list.find(dsn::gpid(3, 1)) != req.confirm_list.end());
        ASSERT_TRUE(req.confirm_list.find(dsn::gpid(5, 1)) != req.confirm_list.end());
    }

    {
        dup_map.erase(3);
        dup_impl->update_duplication_map(dup_map);
        ASSERT_TRUE(find_dup(stub->find_replica(3, 1), 2) == nullptr);
    }
}

// This test ensures that dups belonging to non-existing apps will be ignored.
// We also test that updates on non-primary replica will be ignored.
TEST_F(replica_stub_duplication_test, update_dups_in_non_existing_apps)
{
    stub->add_non_primary_replica(2);

    duplication_entry ent;
    ent.dupid = 1;
    ent.status = duplication_status::DS_PAUSE;

    std::map<int32_t, std::vector<duplication_entry>> dup_map;
    dup_map[1].push_back(ent); // app 1 is not existed
    dup_map[2].push_back(ent); // app 2 doesn't have a primary replica

    dup_impl->update_duplication_map(dup_map);
}

TEST_F(replica_stub_duplication_test, update_confirmed_points)
{
    for (int32_t appid = 1; appid <= 10; appid++) {
        stub->add_primary_replica(appid, 1);
    }

    for (int appid = 1; appid <= 3; appid++) {
        auto r = stub->find_replica(appid, 1);

        duplication_entry ent;
        ent.dupid = 1;
        auto dup = std::make_shared<mutation_duplicator>(ent, r);
        dup->update_state(dup->view().set_last_decree(3).set_confirmed_decree(1));
        add_dup(r, std::move(dup));
    }

    auto req = dsn::make_unique<duplication_sync_request>();
    duplication_confirm_entry confirm;
    confirm.dupid = 1;
    confirm.confirmed_decree = 3;
    req->confirm_list[dsn::gpid(1, 1)].push_back(confirm);
    req->confirm_list[dsn::gpid(2, 1)].push_back(confirm);
    req->confirm_list[dsn::gpid(3, 1)].push_back(confirm);

    duplication_sync_rpc rpc(std::move(req), RPC_CM_DUPLICATION_SYNC);

    duplication_entry ent;
    ent.dupid = 1;
    ent.confirmed_decree = 3;
    rpc.response().dup_map[1].push_back(ent);
    rpc.response().dup_map[2].push_back(ent);
    rpc.response().dup_map[3].push_back(ent);

    dup_impl->on_duplication_sync_reply(dsn::ERR_OK, rpc);

    for (int appid = 1; appid <= 3; appid++) {
        auto r = stub->find_replica(appid, 1);
        auto dup = find_dup(r, 1);

        ASSERT_EQ(dup->view().confirmed_decree, 3);
    }
}

TEST_F(replica_stub_duplication_test, on_duplication_sync_reply)
{
    test_on_duplication_sync_reply();
}

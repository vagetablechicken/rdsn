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

#include "duplication_test_base.h"

namespace dsn {
namespace replication {

struct replica_duplication_test : public replica_duplication_test_base
{
    void SetUp() override { stub = dsn::make_unique<mock_replica_stub>(); }

    void TearDown() override { stub.reset(); }

    void test_remove_non_existed_duplications()
    {
        auto r = stub->add_primary_replica(2, 1);
        auto &d = r->get_replica_duplication_impl();

        duplication_entry ent;
        ent.dupid = 1;
        ent.status = duplication_status::DS_PAUSE;
        ent.remote_address = "dsn://slave-cluster";
        d.sync_duplication(ent);
        ASSERT_EQ(d._duplications.size(), 1);

        // remove all dup
        std::vector<duplication_entry> empty_list;
        d.remove_non_existed_duplications(empty_list);
        ASSERT_EQ(d._duplications.size(), 0);

        ent.dupid = 2;
        d.sync_duplication(ent);
        ASSERT_EQ(d._duplications.size(), 1);
    }
};

TEST_F(replica_duplication_test, get_duplication_confirms)
{
    auto r = stub->add_primary_replica(2, 1);

    int total_dup_num = 10;
    int update_dup_num = 4; // the number of dups that will be updated

    for (dupid_t id = 1; id <= update_dup_num; id++) {
        duplication_entry ent;
        ent.dupid = id;

        auto dup = dsn::make_unique<mutation_duplicator>(ent, r);
        dup->update_state(dup->view().set_last_decree(2).set_confirmed_decree(1));
        add_dup(r, std::move(dup));
    }

    for (dupid_t id = update_dup_num + 1; id <= total_dup_num; id++) {
        duplication_entry ent;
        ent.dupid = id;

        auto dup = dsn::make_unique<mutation_duplicator>(ent, r);
        dup->update_state(dup->view().set_last_decree(1).set_confirmed_decree(1));
        add_dup(r, std::move(dup));
    }

    auto result = r->get_replica_duplication_impl().get_duplication_confirms_to_update();
    ASSERT_EQ(result.size(), update_dup_num);
}

TEST_F(replica_duplication_test, remove_non_existed_duplications)
{
    test_remove_non_existed_duplications();
}

TEST_F(replica_duplication_test, min_confirmed_decree) {}

} // namespace dsn
} // namespace replication

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

using mutation_batch = mutation_duplicator::mutation_batch;

struct mutation_duplicator_test : public duplication_test_base
{
    static void write_single_log_file(const std::vector<std::string> &muts)
    {
        // prepare
        utils::filesystem::remove_path(logp);
        utils::filesystem::create_directory(logp);

        // writing logs
        mutation_log_ptr mlog =
            new mutation_log_private(logp, 4, replica_gpid, nullptr, 1024, 512, 10000);
        ASSERT_EQ(mlog->open(nullptr, nullptr), ERR_OK);

        for (int i = 0; i < muts.size(); i++) {
            auto mu = create_test_mutation(i + 1, muts[i]);
            mlog->append(mu, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr, 0);
        }
    }

    static void ASSERT_MUTATIONS_EQ(const mutation_duplicator &duplicator,
                                    const std::vector<std::string> &expected)
    {
        std::vector<std::string> actual;
        for (auto &batch : duplicator._mutation_batches) {
            mutation_batch_to_vec_string(*batch, &actual);
        }
        ASSERT_EQ(actual, expected);
    }

    static void mutation_batch_to_vec_string(const mutation_batch &batch,
                                             std::vector<std::string> *dest)
    {
        mutation_batch tmp(0);
        auto new_mutation_cache = batch._mutations.copy();
        while (true) {
            auto mu = new_mutation_cache->pop_min();
            if (mu == nullptr) {
                break;
            }
            tmp.add(1, mu);
        }

        std::vector<dsn_message_t> vec_message;
        mutation_duplicator::mutation_batch_to_vec_message(&tmp, &vec_message);
        for (dsn_message_t msg : vec_message) {
            dest->emplace_back(message_t_to_string(msg));
        }
    }

    static mutation_ptr create_test_mutation(int64_t decree, const std::string &data)
    {
        mutation_ptr mu(new mutation());
        mu->data.header.ballot = 1;
        mu->data.header.decree = decree;
        mu->data.header.pid = replica_gpid;
        mu->data.header.last_committed_decree = decree - 1;
        mu->data.header.log_offset = 0;

        binary_writer writer;
        writer.write(data);

        mu->data.updates.emplace_back(mutation_update());
        mu->data.updates.back().code = RPC_REPLICATION_WRITE_EMPTY;
        mu->data.updates.back().data = writer.get_buffer();
        mu->client_requests.push_back(nullptr);

        // mutation_duplicator always loads from hard disk,
        // so it must be logged.
        mu->set_logged();

        return mu;
    }

    static const prepare_list &get_prepare_list(const mutation_batch &batch)
    {
        return batch._mutations;
    }

    static std::unique_ptr<mutation_duplicator> create_duplicator(replica *r)
    {
        duplication_entry dup_ent;
        dup_ent.dupid = 1;
        dup_ent.remote_address = "remote_address";
        dup_ent.status = duplication_status::DS_START;
        dup_ent.confirmed_decree = 100;
        return make_unique<mutation_duplicator>(dup_ent, r);
    }

    static std::string message_t_to_string(dsn_message_t req)
    {
        void *s;
        size_t len;

        dsn_msg_read_next(req, &s, &len);
        blob bb((char *)s, 0, len);
        binary_reader reader(bb);

        std::string data;
        reader.read(data);
        return data;
    }

    static const std::string logp;
    static const gpid replica_gpid;
};

/* static */ const std::string mutation_duplicator_test::logp = "./test-log";
/* static */ const gpid mutation_duplicator_test::replica_gpid(1, 0);

struct mock_duplication_backlog_handler : public duplication_backlog_handler
{
    error_s duplicate(std::vector<dsn_message_t> *mutations) override
    {
        for (dsn_message_t req : *mutations) {
            mutation_list.emplace_back(mutation_duplicator_test::message_t_to_string(req));
        }

        return error_s::ok();
    }

    std::vector<std::string> mutation_list;
};

TEST_F(mutation_duplicator_test, new_duplicator)
{
    auto stub = make_unique<replica_stub>();
    auto r = create_replica(stub.get());

    dupid_t dupid = 1;
    std::string remote_address = "remote_address";
    duplication_status::type status = duplication_status::DS_START;
    int64_t confirmed_decree = 100;

    duplication_entry dup_ent;
    dup_ent.dupid = dupid;
    dup_ent.remote_address = remote_address;
    dup_ent.status = status;
    dup_ent.confirmed_decree = confirmed_decree;

    auto duplicator = make_unique<mutation_duplicator>(dup_ent, r.get());
    ASSERT_EQ(duplicator->view().id, dupid);
    ASSERT_EQ(duplicator->view().status, status);
    ASSERT_EQ(duplicator->view().remote_cluster_address, remote_address);
    ASSERT_EQ(duplicator->view().confirmed_decree, confirmed_decree);
    ASSERT_EQ(duplicator->view().last_decree, confirmed_decree);
}

TEST_F(mutation_duplicator_test, mutation_batch)
{
    {
        mutation_batch batch(0);
        int max_size = mutation_batch::per_batch_num_entries;
        for (int i = 1; i <= max_size; i++) {
            auto es = batch.add(3, create_test_mutation(i, "abc"));
            ASSERT_TRUE(es.is_ok()) << es.description();
            ASSERT_EQ(get_prepare_list(batch).count(), i);
        }
        ASSERT_TRUE(batch.oversize()); // exceed number limit
    }

    {
        mutation_batch batch(0);
        int max_size = mutation_batch::per_batch_size_bytes;
        batch.add(max_size, create_test_mutation(1, "abc"));
        ASSERT_FALSE(batch.oversize());

        batch.add(1, create_test_mutation(2, "abc"));
        ASSERT_TRUE(batch.oversize()); // exceed space limit
    }
}

TEST_F(mutation_duplicator_test, load_mutations_from_log_file_and_ship_mutations)
{
    struct TestData
    {
        int num_entries;
    } tests[] = {
        {mutation_batch::per_batch_num_entries / 10},
        {mutation_batch::per_batch_num_entries + 1},
        {mutation_batch::per_batch_num_entries * 2},
    };

    auto stub = make_unique<replica_stub>();
    auto r = create_replica(stub.get());
    r->get_app()->set_duplication_backlog_handler(new mock_duplication_backlog_handler);

    for (auto tt : tests) {
        // write test logs
        std::vector<std::string> muts;
        for (int i = 0; i < tt.num_entries; i++) {
            muts.emplace_back(std::to_string(i));
        }
        write_single_log_file(muts);

        // read from log file
        error_code ec;
        std::string log_f_name = logp + fmt::format("/log.{}.{}",
                                                    replica_gpid.get_app_id(),
                                                    replica_gpid.get_partition_index());
        auto logf = log_file::open_read(log_f_name.c_str(), ec);
        ASSERT_EQ(ec, ERR_OK);
        auto duplicator = create_duplicator(r.get());
        duplicator->load_mutations_from_log_file(logf);

        // load_mutations_from_log_file must read all mutations written before
        ASSERT_MUTATIONS_EQ(*duplicator, muts);

        while (duplicator->ship_mutations())
            ;

        // all mutations must have been shipped now.
        ASSERT_MUTATIONS_EQ(*duplicator, std::vector<std::string>());

        auto dbh = dynamic_cast<mock_duplication_backlog_handler *>(
            r->get_app()->get_duplication_backlog_handler());
        ASSERT_EQ(dbh->mutation_list, muts);

        dbh->mutation_list.clear();
    }
}

} // namespace replication
} // namespace dsn
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

inline std::string dsn_message_t_to_string(dsn_message_t req)
{
    req = dsn_msg_copy(req, true, false);

    void *s;
    size_t len;

    dsn_msg_read_next(req, &s, &len);
    blob bb((char *)s, 0, len);
    binary_reader reader(bb);

    std::string data;
    reader.read(data);
    dsn_msg_read_commit(req, len);

    return data;
}

struct mock_duplication_backlog_handler : public duplication_backlog_handler
{
    // thread-safe
    error_s duplicate(std::vector<dsn_message_t> *mutations) override
    {
        zauto_lock _(lock);
        for (dsn_message_t req : *mutations) {
            mutation_list.emplace_back(dsn_message_t_to_string(req));
        }
        return error_s::ok();
    }

    // thread-safe
    std::vector<std::string> get_mutation_list_safe()
    {
        zauto_lock _(lock);
        return mutation_list;
    }

    std::vector<std::string> mutation_list;
    mutable zlock lock;
};

struct mutation_duplicator_test : public duplication_test_base
{
    using mutation_batch = mutation_duplicator::mutation_batch;

    mutation_duplicator_test() : log_dir("./test-log")
    {
        stub = make_unique<replica_stub>();
        replica = create_replica(stub.get(), 1, 1, log_dir.c_str());
        backlog_handler = new mock_duplication_backlog_handler;
        replica->get_app()->set_duplication_backlog_handler(backlog_handler);
    }

    void SetUp() override
    {
        utils::filesystem::remove_path(log_dir);
        utils::filesystem::create_directory(log_dir);
    }

    void TearDown() override
    {
        //        utils::filesystem::remove_path(log_dir);
        backlog_handler->mutation_list.clear();
    }

    void ASSERT_MUTATIONS_EQ(const mutation_duplicator &duplicator,
                             const std::vector<std::string> &expected)
    {
        std::vector<std::string> actual;
        for (dsn_message_t msg : duplicator._mutation_batch->_mutations) {
            actual.emplace_back(dsn_message_t_to_string(msg));
        }
        ASSERT_EQ(actual.size(), expected.size());

        for (int i = 0; i < actual.size(); i++) {
            ASSERT_EQ(actual[i], expected[i]) << i << " " << actual[i] << " vs " << expected[i];
        }
    }

    mutation_ptr create_test_mutation(int64_t decree, const std::string &data)
    {
        mutation_ptr mu(new mutation());
        mu->data.header.ballot = 1;
        mu->data.header.decree = decree;
        mu->data.header.pid = replica->get_gpid();
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

    std::unique_ptr<mutation_duplicator> create_test_duplicator()
    {
        duplication_entry dup_ent;
        dup_ent.dupid = 1;
        dup_ent.remote_address = "remote_address";
        dup_ent.status = duplication_status::DS_START;
        dup_ent.confirmed_decree = 0;
        return make_unique<mutation_duplicator>(dup_ent, replica.get());
    }

    void test_load_and_ship_mutations(int num_entries)
    {
        std::vector<std::string> mutations;

        { // writing logs
            mutation_log_ptr mlog = new mutation_log_private(
                log_dir, 1024, replica->get_gpid(), nullptr, 1024, 512, 10000);
            ASSERT_EQ(mlog->open(nullptr, nullptr), ERR_OK);

            for (int i = 0; i < num_entries; i++) {
                std::string msg = "hello!";
                mutations.push_back(msg);
                mutation_ptr mu = create_test_mutation(2 + i, msg);
                mlog->append(mu, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr, 0);
            }

            // commit the last entry
            mutation_ptr mu = create_test_mutation(2 + num_entries, "hello!");
            mlog->append(mu, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr, 0);

            dsn_task_tracker_wait_all(mlog->tracker());
        }

        { // read from log file
            auto logf = log_utils::open_read_or_die(log_dir + "/log.1.0");

            auto duplicator = create_test_duplicator();
            while (duplicator->load_mutations_from_log_file(logf)) {
            }

            // load_mutations_from_log_file must read all mutations written before
            ASSERT_MUTATIONS_EQ(*duplicator, mutations);

            {
                duplicator->_paused = false; // set _paused to false to be able to start shipping.
                auto messages = duplicator->_mutation_batch->move_to_vec_message();

                // pause immediately after shipping finishes.
                tasking::enqueue(LPC_DUPLICATE_MUTATIONS,
                                 duplicator->tracker(),
                                 [&duplicator, &messages]() {
                                     duplicator->ship_mutations(messages);
                                     duplicator->pause();
                                 },
                                 gpid_to_thread_hash(replica->get_gpid()));
                duplicator->wait_all();
            }

            // all mutations must have been shipped now.
            ASSERT_MUTATIONS_EQ(*duplicator, std::vector<std::string>());

            ASSERT_EQ(backlog_handler->mutation_list, mutations)
                << backlog_handler->mutation_list.size() << " vs " << mutations.size();
        }
    }

    void test_start_duplication(int num_entries, int private_log_size_mb)
    {
        std::vector<std::string> mutations;

        mutation_log_ptr mlog = new mutation_log_private(
            replica->dir(), private_log_size_mb, replica->get_gpid(), nullptr, 1024, 512, 10000);
        EXPECT_EQ(mlog->open(nullptr, nullptr), ERR_OK);

        { // writing mutations that only generate 1 log file.
            for (int i = 0; i < num_entries; i++) {
                std::string msg = "hello!";
                mutations.push_back(msg);
                mutation_ptr mu = create_test_mutation(2 + i, msg);
                mlog->append(mu, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr, 0);
            }

            // commit the last entry
            mutation_ptr mu = create_test_mutation(2 + num_entries, "hello!");
            mlog->append(mu, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr, 0);

            dsn_task_tracker_wait_all(mlog->tracker());
        }

        {
            replica->init_private_log(mlog);
            auto duplicator = create_test_duplicator();
            duplicator->start();

            while (backlog_handler->get_mutation_list_safe().size() < mutations.size()) {
                sleep(1);
            }

            duplicator->pause();
            duplicator->wait_all();

            ASSERT_EQ(backlog_handler->mutation_list, mutations)
                << backlog_handler->mutation_list.size() << " vs " << mutations.size();
        }
    }

    const std::string log_dir;

    std::unique_ptr<mock_replica> replica;
    std::unique_ptr<replica_stub> stub;
    mock_duplication_backlog_handler *backlog_handler;
};

TEST_F(mutation_duplicator_test, new_duplicator)
{
    dupid_t dupid = 1;
    std::string remote_address = "remote_address";
    duplication_status::type status = duplication_status::DS_START;
    int64_t confirmed_decree = 100;

    duplication_entry dup_ent;
    dup_ent.dupid = dupid;
    dup_ent.remote_address = remote_address;
    dup_ent.status = status;
    dup_ent.confirmed_decree = confirmed_decree;

    auto duplicator = make_unique<mutation_duplicator>(dup_ent, replica.get());
    ASSERT_EQ(duplicator->id(), dupid);
    ASSERT_EQ(duplicator->remote_cluster_address(), remote_address);
    ASSERT_EQ(duplicator->view().status, status);
    ASSERT_EQ(duplicator->view().confirmed_decree, confirmed_decree);
    ASSERT_EQ(duplicator->view().last_decree, confirmed_decree);
}

TEST_F(mutation_duplicator_test, load_and_ship_mutations_1000)
{
    test_load_and_ship_mutations(1000);
}

TEST_F(mutation_duplicator_test, load_and_ship_mutations_2000)
{
    test_load_and_ship_mutations(2000);
}

TEST_F(mutation_duplicator_test, load_and_ship_mutations_5000)
{
    test_load_and_ship_mutations(5000);
}

TEST_F(mutation_duplicator_test, load_and_ship_mutations_10000)
{
    test_load_and_ship_mutations(10000);
}

TEST_F(mutation_duplicator_test, find_log_file_with_min_index)
{
    std::vector<std::string> mutations;
    int max_log_file_mb = 1;

    mutation_log_ptr mlog = new mutation_log_private(
        replica->dir(), max_log_file_mb, replica->get_gpid(), nullptr, 1024, 512, 10000);
    EXPECT_EQ(mlog->open(nullptr, nullptr), ERR_OK);

    { // writing mutations to log which will generate multiple files
        for (int i = 0; i < 1000 * 20; i++) {
            std::string msg = "hello!";
            mutations.push_back(msg);
            mutation_ptr mu = create_test_mutation(2 + i, msg);
            mlog->append(mu, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr, 0);
        }
    }

    auto files = log_utils::list_all_files_or_die(log_dir);
    auto lf = mutation_duplicator::find_log_file_with_min_index(files);
    ASSERT_TRUE(lf != nullptr);
    ASSERT_EQ(lf->index(), 1);
}

TEST_F(mutation_duplicator_test, start_duplication_10000_4MB) { test_start_duplication(10000, 4); }

TEST_F(mutation_duplicator_test, start_duplication_50000_4MB) { test_start_duplication(50000, 4); }

TEST_F(mutation_duplicator_test, start_duplication_10000_1MB) { test_start_duplication(10000, 1); }

TEST_F(mutation_duplicator_test, start_duplication_50000_1MB) { test_start_duplication(50000, 1); }

// Ensures no tasks will be running after duplicator was paused.
TEST_F(mutation_duplicator_test, pause_start_duplication)
{
    mutation_log_ptr mlog =
        new mutation_log_private(replica->dir(), 4, replica->get_gpid(), nullptr, 1024, 512, 10000);
    EXPECT_EQ(mlog->open(nullptr, nullptr), ERR_OK);

    {
        replica->init_private_log(mlog);
        auto duplicator = create_test_duplicator();
        duplicator->start();
        duplicator->pause();

        duplicator->wait_all();
    }
}

TEST_F(mutation_duplicator_test, duplication_view)
{
    auto duplicator = create_test_duplicator();
    ASSERT_EQ(duplicator->view().last_decree, 0);
    ASSERT_EQ(duplicator->view().confirmed_decree, 0);

    duplicator->update_state(duplicator->view().set_last_decree(10));
    ASSERT_EQ(duplicator->view().last_decree, 10);
    ASSERT_EQ(duplicator->view().confirmed_decree, 0);

    duplicator->update_state(duplicator->view().set_confirmed_decree(10));
    ASSERT_EQ(duplicator->view().confirmed_decree, 10);
    ASSERT_EQ(duplicator->view().last_decree, 10);
}

} // namespace replication
} // namespace dsn
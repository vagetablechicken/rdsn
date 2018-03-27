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

#include "dist/replication/lib/duplication/private_log_loader.h"

#include "duplication_test_base.h"

namespace dsn {
namespace replication {

struct mock_stage : pipeline::when<mutation_tuple_set>
{
    explicit mock_stage(std::function<void(mutation_tuple_set &&)> &&func) : _cb(std::move(func)) {}

    void run(mutation_tuple_set &&mutations) override { _cb(std::move(mutations)); }

    std::function<void(mutation_tuple_set &&)> _cb;
};

struct load_from_private_log_test : public mutation_duplicator_test_base
{
    load_from_private_log_test() : duplicator(create_test_duplicator())
    {
        utils::filesystem::remove_path(log_dir);
        utils::filesystem::create_directory(log_dir);
    }

    void test_find_log_file_to_start()
    {
        load_from_private_log load(replica.get());

        std::vector<std::string> mutations;
        int max_log_file_mb = 1;

        mutation_log_ptr mlog = new mutation_log_private(
            replica->dir(), max_log_file_mb, replica->get_gpid(), nullptr, 1024, 512, 10000);
        EXPECT_EQ(mlog->open(nullptr, nullptr), ERR_OK);

        { // writing mutations to log which will generate multiple files
            for (int i = 0; i < 1000 * 50; i++) {
                std::string msg = "hello!";
                mutations.push_back(msg);
                mutation_ptr mu = create_test_mutation(2 + i, msg);
                mlog->append(mu, LPC_AIO_IMMEDIATE_CALLBACK, nullptr, nullptr, 0);
            }
        }

        auto files = log_utils::list_all_files_or_die(log_dir);

        load.set_start_decree(10);
        load.find_log_file_to_start(files);
        auto lf = load._current;
        ASSERT_TRUE(lf);
        ASSERT_EQ(lf->index(), 1);

        load.set_start_decree(500);
        load.find_log_file_to_start(files);
        lf = load._current;
        ASSERT_TRUE(lf);
        ASSERT_EQ(lf->index(), 1);

        std::map<int, log_file_ptr> log_file_map = log_utils::open_log_file_map(files);
        int last_idx = log_file_map.rbegin()->first;
        load.set_start_decree(1000 * 50 + 200);
        load.find_log_file_to_start(files);
        lf = load._current;
        ASSERT_TRUE(lf);
        ASSERT_EQ(lf->index(), last_idx);
    }

    // Ensure mutation_duplicator can correctly handle real-world log file (log.1.0).
    // There are 4 puts, 3 write empties in log.1.0: PUT, PUT, PUT, EMPTY, PUT, EMPTY, EMPTY.
    void test_handle_real_private_log()
    {
        constexpr int total_writes_size = 4;
        constexpr int total_mutations_size = 7;

        ASSERT_TRUE(utils::filesystem::rename_path("log.1.0", log_dir + "/log.1.0"));

        {
            /// load log.1.0
            mutation_log_ptr mlog = new mutation_log_private(
                replica->dir(), 4, replica->get_gpid(), nullptr, 1024, 512, 10000);
            replica->init_private_log(mlog);
            mlog->update_max_commit_on_disk(total_mutations_size); // assume all logs are committed.
        }

        load_and_wait_all_entries_loaded(7);
    }

    void test_start_duplication(int num_entries, int private_log_size_mb)
    {
        std::vector<std::string> mutations;

        mutation_log_ptr mlog = new mutation_log_private(
            replica->dir(), private_log_size_mb, replica->get_gpid(), nullptr, 1024, 512, 50000);
        EXPECT_EQ(mlog->open(nullptr, nullptr), ERR_OK);
        replica->init_private_log(mlog);

        {
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

        load_and_wait_all_entries_loaded(num_entries);
    }

    mutation_tuple_set load_and_wait_all_entries_loaded(int total)
    {
        load_from_private_log load(replica.get());
        mutation_tuple_set loaded_mutations;
        mock_stage end_stage([&loaded_mutations, &load, total](mutation_tuple_set &&mutations) {
            for (mutation_tuple mut : mutations) {
                loaded_mutations.emplace(mut);
            }

            if (loaded_mutations.size() < total) {
                load.run();
            }
        });

        duplicator->from(&load).link(&end_stage);
        duplicator->run_pipeline();
        duplicator->wait_all();

        return loaded_mutations;
    }

    std::unique_ptr<mutation_duplicator> duplicator;
};

TEST_F(load_from_private_log_test, find_log_file_to_start) { test_find_log_file_to_start(); }

TEST_F(load_from_private_log_test, handle_real_private_log) { test_handle_real_private_log(); }

TEST_F(load_from_private_log_test, start_duplication_10000_4MB)
{
    test_start_duplication(10000, 4);
}

TEST_F(load_from_private_log_test, start_duplication_50000_4MB)
{
    test_start_duplication(50000, 4);
}

TEST_F(load_from_private_log_test, start_duplication_10000_1MB)
{
    test_start_duplication(10000, 1);
}

TEST_F(load_from_private_log_test, start_duplication_50000_1MB)
{
    test_start_duplication(50000, 1);
}

} // namespace replication
} // namespace dsn

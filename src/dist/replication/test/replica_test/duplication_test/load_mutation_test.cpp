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

#include "dist/replication/lib/duplication/mutation_batch.h"
#include "dist/replication/lib/duplication/duplication_pipeline.h"

#include "duplication_test_base.h"

namespace dsn {
namespace replication {

struct mock_stage : pipeline::when<decree, mutation_tuple_set>
{
    explicit mock_stage(std::function<void(decree &&, mutation_tuple_set &&)> &&func)
        : _cb(std::move(func))
    {
    }

    void run(decree &&d, mutation_tuple_set &&mutations) override
    {
        _cb(std::move(d), std::move(mutations));
    }

    std::function<void(decree &&, mutation_tuple_set &&)> _cb;
};

struct load_mutation_test : public mutation_duplicator_test_base
{
    load_mutation_test() : duplicator(create_test_duplicator()) {}

    void test_load_mutation_from_cache()
    {
        mutation_batch batch;
        batch.add(create_test_mutation(1, "hello"));
        batch.add(create_test_mutation(2, "world"));
        batch.add(create_test_mutation(3, "")); // commit to 2

        { // initiates private log.
            mutation_log_ptr mlog = new mutation_log_private(
                replica->dir(), 4, replica->get_gpid(), nullptr, 1024, 512, 10000);
            replica->init_private_log(mlog);
            mlog->update_max_commit_on_disk(2); // assume all logs are committed.
        }

        load_mutation loader(duplicator.get(), batch._mutation_buffer.get());
        mock_stage end([](decree &&d, mutation_tuple_set &&mutations) {
            ASSERT_EQ(d, 2);
            ASSERT_EQ(mutations.size(), 2);

            auto it = mutations.begin();
            ASSERT_EQ(std::get<0>(*it), 1);

            it = std::next(it);
            ASSERT_EQ(std::get<0>(*it), 2);
        });

        pipeline::base base;
        base.thread_pool(LPC_DUPLICATION_LOAD_MUTATIONS)
            .task_tracker(replica.get())
            .from(&loader)
            .link(&end);

        loader.run();

        base.wait_all();
    }

    std::unique_ptr<mutation_duplicator> duplicator;
};

TEST_F(load_mutation_test, load_mutation_from_cache) { test_load_mutation_from_cache(); }

} // namespace replication
} // namespace dsn

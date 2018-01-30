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

#include <gtest/gtest.h>

#include "dist/replication/lib/prepare_list.h"

using namespace dsn::replication;

class prepare_list_test : public ::testing::Test
{
public:
};

mutation_ptr new_mut(int64_t decree, int64_t ballot, int64_t last_committed)
{
    mutation_ptr mut = new mutation;
    mut->data.header.decree = decree;
    mut->data.header.ballot = ballot;
    mut->data.header.last_committed_decree = last_committed;
    mut->set_logged();
    return mut;
}

TEST_F(prepare_list_test, prepare_PS_INACTIVE)
{
    struct TestData
    {
        std::vector<mutation_ptr> muts;

        int64_t wlast;
        int64_t wcommit;
    } tests[] = {
        {{new_mut(1, 1, 0), new_mut(2, 2, 1), new_mut(3, 3, 2)}, 3, 2},

        // Never possible, outdated mutations should be ignored before adding to prepare_list.
        // {new_mut(1, 1, 0), new_mut(4, 1, 3), new_mut(3, 1, 2), new_mut(2, 1, 1)},

        // Never possible, mutation(1,1,0) violates commit invariant
        // {new_mut(3, 1, 1), new_mut(2, 1, 0), new_mut(1, 1, 0)}
    };

    for (auto tt : tests) {
        prepare_list list(0, 1000, [](mutation_ptr &) {});
        for (mutation_ptr mut : tt.muts) {
            list.prepare(mut, partition_status::PS_INACTIVE);
        }

        ASSERT_EQ(list.max_decree(), tt.wlast);
        ASSERT_EQ(list.last_committed_decree(), tt.wcommit);
    }
}

TEST_F(prepare_list_test, count)
{
    struct TestData
    {
        std::vector<mutation_ptr> muts;

        int64_t wcount;
    } tests[] = {
        {{new_mut(1, 1, 0), new_mut(2, 2, 1), new_mut(3, 3, 2)}, 3},
    };

    for (auto tt : tests) {
        prepare_list list(0, 1000, [](mutation_ptr &) {});
        for (mutation_ptr mut : tt.muts) {
            list.prepare(mut, partition_status::PS_INACTIVE);
        }

        ASSERT_EQ(list.count(), tt.wcount);
    }
}

TEST_F(prepare_list_test, pop_min)
{
    struct TestData
    {
        std::vector<mutation_ptr> muts;

        std::vector<mutation_ptr> popped_muts;
    } tests[] = {
        //        {{new_mut(1, 1, 0), new_mut(2, 2, 1), new_mut(3, 3, 2)},
        //         {new_mut(1, 1, 0), new_mut(2, 2, 1), new_mut(3, 3, 2)}},
    };

    for (auto tt : tests) {
        prepare_list list(0, 1000, [](mutation_ptr &) {});
        for (mutation_ptr mut : tt.muts) {
            list.prepare(mut, partition_status::PS_INACTIVE);
        }

        ASSERT_EQ(list.count(), tt.popped_muts.size());
        for (int i = 0; i < tt.muts.size(); i++) {
            mutation_ptr mu = list.pop_min();

            ASSERT_EQ(mu->get_decree(), tt.popped_muts[i]->get_decree());
            ASSERT_EQ(mu->get_ballot(), tt.popped_muts[i]->get_ballot());
        }
    }
}
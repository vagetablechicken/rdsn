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
#include <dsn/cpp/pipeline.h>
#include <dsn/dist/replication.h>

namespace dsn {

TEST(pipeline_test, pause)
{
    clientlet tracker;

    {
        pipeline::base base;
        ASSERT_TRUE(base.paused());

        base.pause();
        ASSERT_TRUE(base.paused());

        pipeline::do_when<> s1([&s1]() { s1.repeat(1_s); });

        base.thread_pool(LPC_DUPLICATE_MUTATIONS).task_tracker(&tracker).from(&s1);

        {
            base.run_pipeline();
            ASSERT_FALSE(base.paused());

            base.pause();
            ASSERT_TRUE(base.paused());

            base.wait_all();
        }
    }
}

TEST(pipeline_test, link_pipe)
{
    clientlet tracker;

    struct stage2 : pipeline::when<>, pipeline::result<>
    {
        void run() override { step_down_next_stage(); }
    };

    {

        pipeline::base base1;
        pipeline::do_when<> s1([&s1]() { s1.repeat(1_s); });
        base1.thread_pool(LPC_DUPLICATION_LOAD_MUTATIONS).task_tracker(&tracker).from(&s1);

        // base2 executes s2, then executes s1 in another pipeline.
        pipeline::base base2;
        stage2 s2;
        base2.thread_pool(LPC_DUPLICATE_MUTATIONS).task_tracker(&tracker).from(&s2).link_pipe(&s1);

        base2.run_pipeline();

        base1.pause();
        base2.pause();

        base2.wait_all();
    }

}

} // namespace dsn

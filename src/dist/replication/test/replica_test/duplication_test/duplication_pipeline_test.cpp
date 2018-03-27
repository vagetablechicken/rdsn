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

#include "dist/replication/lib/duplication/duplication_pipeline.h"

#include "duplication_test_base.h"

namespace dsn {
namespace replication {

struct ship_mutation_test : public mutation_duplicator_test_base
{
    ship_mutation_test() : duplicator(create_test_duplicator()) {}

    std::unique_ptr<mutation_duplicator> duplicator;
};

struct mock_stage : pipeline::when<>
{
    void run() override {}
};

TEST_F(ship_mutation_test, ship_mutation_tuple_set)
{
    ship_mutation shipper(duplicator.get());
    mock_stage end;

    pipeline::base base;
    base.thread_pool(LPC_DUPLICATION_LOAD_MUTATIONS).from(&shipper).link_0(&end);

    mock_duplication_backlog_handler::mock(
        [](mutation_tuple mut, duplication_backlog_handler::err_callback cb) {

        });

    mutation_tuple_set mutations;

    create_test_mutation(1, "hello");
    create_test_mutation(1, "world");
    shipper.run(std::move(mutations));
}

} // namespace replication
} // namespace dsn

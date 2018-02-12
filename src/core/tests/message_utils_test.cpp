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

#include <dsn/tool-api/rpc_message.h>
#include <dsn/utility/message_utils.h>
#include <gtest/gtest.h>

using namespace dsn;

DEFINE_TASK_CODE_RPC(RPC_CODE_FOR_TEST, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

TEST(message_utils, msg_blob_convertion)
{
    std::string data = "hello";

    blob b(data.c_str(), 0, data.size());
    dsn_message_t m = copy_blob_to_dsn_message_t(b, RPC_CODE_FOR_TEST);

    ASSERT_EQ(dsn_msg_body_size(m), data.size());

    // copy for read
    m = dsn_msg_copy(m, true, true);
    ASSERT_EQ(b, move_dsn_message_t_to_blob(m));
}
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

#pragma once

#include <dsn/utility/blob.h>
#include <dsn/c/api_common.h>
#include <dsn/tool/cli.h>

namespace dsn {

/// Move the content inside message `m` into a blob.
extern blob move_dsn_message_t_to_blob(dsn_message_t m);

/// Move the data inside blob `b` into a message for reading(unmarshalling).
/// This function is identical with dsn_msg_create_received_request,
/// but the internal data used to create message will be moved
/// rather than be simply referenced.
/// MUST released mannually later using dsn_msg_release_ref.
extern dsn_message_t move_blob_to_received_message(dsn_task_code_t rpc_code,
                                                   blob &&bb,
                                                   int thread_hash DEFAULT(0),
                                                   uint64_t partition_hash DEFAULT(0));

/// Convert a thrift request into a dsn message (using binary encoding).
/// When to use this:
///     1. Unit test: when we create a fake message as the function argument.
template <typename T>
inline dsn_message_t from_thrift_request_to_received_message(const T &request, dsn_task_code_t tc)
{
    dsn::binary_writer writer;
    dsn::marshall_thrift_binary(writer, request);
    return move_blob_to_received_message(tc, writer.get_buffer());
}

} // namespace dsn
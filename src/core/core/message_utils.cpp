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

#include <dsn/c/api_layer1.h>
#include <dsn/utility/binary_reader.h>
#include <dsn/utility/message_utils.h>
#include <dsn/cpp/auto_codes.h>
#include <dsn/utility/binary_writer.h>

// Header file: dsn/utility/message_utils.h

namespace dsn {

/*extern*/ blob move_dsn_message_t_to_blob(dsn_message_t m)
{
    void *ptr;
    size_t len;

    dsn_msg_read_next(m, &ptr, &len);
    dsn_msg_read_commit(m, len);

    return dsn::blob((char *)ptr, 0, static_cast<unsigned int>(len));
}

/*extern*/ dsn_message_t move_blob_to_received_message(dsn_task_code_t rpc_code,
                                                       blob &&bb,
                                                       int thread_hash,
                                                       uint64_t partition_hash)
{
    auto msg = ::dsn::message_ex::create_receive_message_with_standalone_header(bb);
    msg->local_rpc_code = rpc_code;
    const char *name = dsn_task_code_to_string(rpc_code);
    strncpy(msg->header->rpc_name, name, strlen(name));

    msg->header->client.thread_hash = thread_hash;
    msg->header->client.partition_hash = partition_hash;
    msg->header->context.u.serialize_format = DSF_THRIFT_BINARY;
    msg->add_ref(); // released by callers explicitly using dsn_msg_release
    return msg;
}

} // namespace dsn
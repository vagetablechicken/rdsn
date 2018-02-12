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

// Header file: dsn/utility/message_utils.h

namespace dsn {

/*extern*/ dsn::blob move_dsn_message_t_to_blob(dsn_message_t m)
{
    void *ptr;
    size_t len;

    dsn_msg_read_next(m, &ptr, &len);
    dsn_msg_read_commit(m, len);

    return dsn::blob((char *)ptr, 0, static_cast<unsigned int>(len));
}

/*extern*/ dsn_message_t copy_blob_to_dsn_message_t(const dsn::blob &b, dsn_task_code_t tc)
{
    dsn_message_t msg = dsn_msg_create_request(tc);

    void *ptr;
    size_t sz;
    dsn_msg_write_next(msg, &ptr, &sz, b.length());

    memcpy(ptr, b.data(), b.length());
    dsn_msg_write_commit(msg, b.length());

    return msg;
}

} // namespace dsn
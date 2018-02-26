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

#include <dsn/dist/replication/duplication_backlog_handler.h>
#include <dsn/cpp/zlocks.h>
#include <dsn/utility/message_utils.h>

namespace dsn {
namespace replication {

inline std::string dsn_message_t_to_string(dsn_message_t req)
{
    req = dsn_msg_copy(req, true, false);
    blob bb = dsn::move_message_to_blob(req);

    binary_reader reader(bb);
    std::string data;
    reader.read(data);
    return data;
}

struct mock_duplication_backlog_handler : public duplication_backlog_handler
{
    // thread-safe
    void duplicate(mutation_tuple mut, err_callback cb) override
    {
        std::lock_guard<std::mutex> _(lock);
        mutation_list.emplace_back(dsn_message_t_to_string(std::get<1>(mut)));
        cb(error_s::ok());
    }

    // thread-safe
    std::vector<std::string> get_mutation_list_safe()
    {
        std::lock_guard<std::mutex> _(lock);
        return mutation_list;
    }

    std::vector<std::string> mutation_list;
    mutable std::mutex lock;
};

struct mock_duplication_backlog_handler_factory : public duplication_backlog_handler_factory
{
    std::unique_ptr<duplication_backlog_handler> create(const std::string &remote_cluster_address,
                                                        const std::string &app) override
    {
        return dsn::make_unique<mock_duplication_backlog_handler>();
    }
};

} // namespace replication
} // namespace dsn

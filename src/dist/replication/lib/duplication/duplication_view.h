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

#include <dsn/dist/replication/duplication_common.h>

namespace dsn {
namespace replication {

class mutation_duplicator;

class duplication_view
{
public:
    const dupid_t id;
    const std::string remote_cluster_address;

    // last decree that's been persisted in meta server
    int64_t confirmed_decree;

    // duplication will start from `_last_decree`,
    // which is the maximum decree that's been duplicated to remote.
    int64_t last_decree;

    duplication_status::type status;

    duplication_view(dupid_t dupid, std::string remote_address)
        : id(dupid),
          remote_cluster_address(std::move(remote_address)),
          status(duplication_status::DS_INIT)
    {
    }

    std::string to_string() const
    {
        return fmt::format(
            "id: {}, remote_cluster_address: {}, confirmed_decree: {}, last_decree: {}, status: {}",
            id,
            remote_cluster_address,
            confirmed_decree,
            last_decree,
            duplication_status_to_string(status));
    }
};

typedef std::unique_ptr<duplication_view> duplication_view_u_ptr;

} // namespace replication
} // namespace dsn

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

#include <dsn/cpp/errors.h>
#include <dsn/dist/replication/replication_types.h>

namespace dsn {
namespace replication {

/// \brief Each of the mutation is a tuple made up of <timestamp, dsn_message_t>.
/// dsn_message_t is the write request serialized by upper-level application.
typedef std::tuple<uint64_t, dsn_message_t> mutation_tuple;

/// \brief This is an interface for handling the mutation logs intended to
/// be duplicated to remote cluster.
/// \see dsn::replication::mutation_duplicator
class duplication_backlog_handler
{
public:
    typedef std::function<void(dsn::error_s)> err_callback;

    /// Duplicate the provided mutation to the remote cluster.
    /// It must be guaranteed the mutations are in the order of the time they applied.
    ///
    /// \param cb: Call it when a specified mutation was sent successfully or
    ///            failed with an error.
    virtual void duplicate(mutation_tuple mutation, err_callback cb) = 0;

    virtual ~duplication_backlog_handler() = default;
};

/// \internal
/// A singleton interface to get duplication_backlog_handler for specified
/// remote cluster and app.
class duplication_backlog_handler_group
{
public:
    virtual duplication_backlog_handler *get(const std::string &remote_cluster_address,
                                             const std::string &app) = 0;

    // thread-safe
    static duplication_backlog_handler_group *get_singleton()
    {
        dassert(_inited && _instance != nullptr,
                "duplication_backlog_handler_group must have been initialized");
        return _instance.get();
    }

    // thread-safe
    static void init_singleton_once(duplication_backlog_handler_group *group)
    {
        std::call_once(_once_flag, [group]() {
            _instance.reset(group);
            _inited = true;
        });
    }

private:
    static std::unique_ptr<duplication_backlog_handler_group> _instance;
    static std::once_flag _once_flag;
    static std::atomic<bool> _inited;
};

/// \brief A helper utility of duplication_backlog_handler_group::get.
inline duplication_backlog_handler *
get_duplication_backlog_handler(const std::string &remote_cluster_address, const std::string &app)
{
    return duplication_backlog_handler_group::get_singleton()->get(remote_cluster_address, app);
}

} // namespace replication
} // namespace dsn
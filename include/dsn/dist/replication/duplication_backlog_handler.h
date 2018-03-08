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

#include <dsn/utility/errors.h>
#include <dsn/dist/replication/replication_types.h>

namespace dsn {
namespace replication {

/// \brief Each of the mutation is a tuple made up of
/// <timestamp, dsn_message_t, dsn::blob>.
/// dsn_message_t is the write request represented as the mutation
/// and dsn::blob is the content read from the message.
typedef std::tuple<uint64_t, dsn_message_t, dsn::blob> mutation_tuple;

struct mutation_tuple_cmp
{
    inline bool operator()(const mutation_tuple &lhs, const mutation_tuple &rhs)
    {
        // different mutations is probable to be batched together
        // and sharing the same timestamp, so here we also compare
        // the message pointer.
        if (std::get<0>(lhs) == std::get<0>(rhs)) {
            return std::get<1>(lhs) < std::get<1>(rhs);
        }
        return std::get<0>(lhs) < std::get<0>(rhs);
    }
};

typedef std::set<mutation_tuple, mutation_tuple_cmp> mutation_tuple_set;

/// \brief This is an interface for handling the mutation logs intended to
/// be duplicated to remote cluster.
/// \see dsn::replication::mutation_duplicator
class duplication_backlog_handler
{
public:
    typedef std::function<void(dsn::error_s)> err_callback;

    /// Duplicate the provided mutation to the remote cluster.
    ///
    /// \param cb: Call it when a specified mutation was sent successfully or
    ///            failed with an error.
    virtual void duplicate(mutation_tuple mutation, err_callback cb) = 0;

    virtual ~duplication_backlog_handler() = default;
};

/// A singleton interface to get duplication_backlog_handler for specified
/// remote cluster and app.
class duplication_backlog_handler_factory
{
public:
    /// Thread-safe
    virtual std::unique_ptr<duplication_backlog_handler>
    create(const std::string &remote_cluster_address, const std::string &app) = 0;

    /// \internal
    /// Thread-safe
    static duplication_backlog_handler_factory *get_singleton()
    {
        dassert(_inited && _instance != nullptr,
                "duplication_backlog_handler_group must have been initialized");
        return _instance.get();
    }

    /// \internal
    /// Thread-safe
    static void init_singleton(duplication_backlog_handler_factory *group)
    {
        std::lock_guard<std::mutex> _(_lock);
        dassert(!_inited, "duplication_backlog_handler_group has been initialized.");
        _instance.reset(group);
        _inited = true;
    }

    /// \internal
    /// \warning This method is only used for unit test.
    static void undo_init() { _inited = false; }

private:
    static std::unique_ptr<duplication_backlog_handler_factory> _instance;
    static std::atomic<bool> _inited;
    static std::mutex _lock;
};

namespace duplication {

/// \brief A helper utility of ::dsn::replication::duplication_backlog_handler_group::create.
inline std::unique_ptr<duplication_backlog_handler>
new_backlog_handler(const std::string &remote_cluster_address, const std::string &app)
{
    return duplication_backlog_handler_factory::get_singleton()->create(remote_cluster_address,
                                                                        app);
}

/// \brief Initializes the backlog_handler_group singleton.
/// Note that this function should be called only once.
inline void init_backlog_handler_factory(duplication_backlog_handler_factory *group)
{
    duplication_backlog_handler_factory::init_singleton(group);
}

} // namespace duplication

} // namespace replication
} // namespace dsn

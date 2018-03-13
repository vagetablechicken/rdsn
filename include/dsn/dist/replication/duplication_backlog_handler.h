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
    explicit duplication_backlog_handler(gpid id) : _gpid(id) {}

    typedef std::function<void(dsn::error_s)> err_callback;

    /// Duplicate the provided mutation to the remote cluster.
    ///
    /// \param cb: Call it when a specified mutation was sent successfully or
    ///            failed with an error.
    virtual void duplicate(mutation_tuple mutation, err_callback cb) = 0;

protected:
    gpid _gpid;
};

/// A singleton interface to get duplication_backlog_handler for specified
/// remote cluster and app.
class duplication_backlog_handler_factory
{
public:
    /// The implementation must be thread-safe.
    virtual std::unique_ptr<duplication_backlog_handler>
    create(gpid id, const std::string &remote_cluster_address, const std::string &app) = 0;

    /// \see dsn::replication::mutation_duplicator
    static duplication_backlog_handler_factory *get_singleton() { return _instance.get(); }

    /// \see dsn::replication::replica_stub_duplication_impl::initialize_and_start()
    static void initialize()
    {
        dassert(_initializer != nullptr, "forget to call set_initializer()?");
        _instance.reset(_initializer());
    }

    typedef std::function<duplication_backlog_handler_factory *()> initializer_func;
    static void set_initializer(initializer_func f) { _initializer = std::move(f); }

private:
    static std::unique_ptr<duplication_backlog_handler_factory> _instance;
    static initializer_func _initializer;
};

/// \brief A helper utility of ::dsn::replication::duplication_backlog_handler_group::create.
inline std::unique_ptr<duplication_backlog_handler>
new_backlog_handler(gpid id, const std::string &remote_cluster_address, const std::string &app)
{
    return duplication_backlog_handler_factory::get_singleton()->create(
        id, remote_cluster_address, app);
}

/// \brief For upper-level application to register their backlog_handler_factory.
/// \return a dummy to allow this function to be called before main.
inline bool
register_backlog_handler_factory(duplication_backlog_handler_factory::initializer_func f)
{
    duplication_backlog_handler_factory::set_initializer(std::move(f));
    return true;
}

} // namespace replication
} // namespace dsn

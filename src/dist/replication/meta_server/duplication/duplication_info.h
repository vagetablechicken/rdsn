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

#include <dsn/dist/replication/replication.types.h>
#include <dsn/dist/replication/duplication_common.h>
#include <dsn/cpp/json_helper.h>
#include <dsn/cpp/zlocks.h>

#include <utility>
#include <fmt/format.h>

namespace dsn {
namespace replication {

using ::dsn::error_code;
using ::dsn::service::zauto_write_lock;

// dupid is the identifier of duplication.
typedef int32_t dupid_t;

class app_state;
class duplication_info
{
public:
    duplication_info(dupid_t dupid, std::string remote_cluster_address, std::string meta_store_path)
        : id(dupid),
          remote(std::move(remote_cluster_address)),
          store_path(std::move(meta_store_path)),
          create_timestamp_ms(dsn_now_ms()),
          _is_altering(false),
          status(duplication_status::DS_INIT),
          next_status(duplication_status::DS_INIT),
          last_progress_update(-1)
    {
    }

    static std::shared_ptr<duplication_info> create_from_blob(const blob &b)
    {
        auto dup = std::shared_ptr<duplication_info>(new duplication_info);
        json::json_forwarder<duplication_info>::decode(b, *dup);
        return dup;
    }

    // Thread-Safe
    void start()
    {
        zauto_write_lock l(lock);
        _is_altering = true;
        next_status = duplication_status::DS_START;
    }

    // change current status to `to`.
    // error will be returned if this state transition is not allowed.
    // Thread-Safe
    error_code alter_status(duplication_status::type to)
    {
        zauto_write_lock l(lock);
        return do_alter_status(to);
    }

    // stable current status to `next_status`
    // call this function after data has been persisted on meta-state storage.
    // Thread-Safe
    void stable_status()
    {
        zauto_write_lock l(lock);
        if (!_is_altering)
            return;

        _is_altering = false;
        status = next_status;
        next_status = duplication_status::DS_INIT;
    }

    // Returns: false if the previous update is still in progress.
    // Thread-Safe
    bool update_progress()
    {
        zauto_write_lock l(lock);
        if (_is_altering) {
            return false;
        }

        _is_altering = true;
        last_progress_update = dsn_now_ms();
        return true;
    }

    // Thread-Safe
    void stable_progress()
    {
        zauto_write_lock l(lock);
        _is_altering = false;
        stored_progress = std::move(progress);
    }

    // Not-Thread-Safe
    bool is_altering() const { return _is_altering; }

    // This function is mainly used for testing.
    bool equals_to(const duplication_info &rhs) const { return to_string() == rhs.to_string(); }

    // This function is mainly used for testing.
    std::string to_string() const
    {
        blob b = json::json_forwarder<duplication_info>::encode(*this);
        return std::string(b.data(), b.length());
    }

    std::shared_ptr<duplication_info> copy()
    {
        blob b = json::json_forwarder<duplication_info>::encode(*this);
        return create_from_blob(b);
    }

    const dupid_t id;
    const std::string remote;
    const std::string store_path;       // store path on meta service
    const uint64_t create_timestamp_ms; // the time when this dup is created.

private:
    duplication_info() : id(0), create_timestamp_ms(0) {}

    error_code do_alter_status(duplication_status::type to);

    // whether the state is changing
    // it will be reset to false after duplication state being persisted.
    bool _is_altering;

public:
    duplication_status::type status;
    duplication_status::type next_status;

    // dupid -> the decree that's been replicated to remote
    std::map<dupid_t, int64_t> progress;
    // the latest progress that's been persisted in meta-state storage
    std::map<dupid_t, int64_t> stored_progress;
    // the time of last progress update to meta-state storage
    uint64_t last_progress_update;

    mutable ::dsn::service::zrwlock_nr lock;

    DEFINE_JSON_SERIALIZATION(id, remote, status, create_timestamp_ms, progress);
};

// format duplication_info using fmt::format"{}"
inline void
format_arg(fmt::BasicFormatter<char> &f, const char *&format_str, const duplication_info &dup)
{
    f.writer() << dup.to_string();
}

typedef std::shared_ptr<duplication_info> duplication_info_s_ptr;

} // namespace replication
} // namespace dsn
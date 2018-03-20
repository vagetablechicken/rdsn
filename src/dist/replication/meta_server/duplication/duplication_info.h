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
#include <dsn/dist/replication/replication_other_types.h>
#include <dsn/dist/replication/duplication_common.h>
#include <dsn/cpp/json_helper.h>
#include <dsn/cpp/zlocks.h>

#include <utility>
#include <fmt/format.h>

namespace dsn {
namespace replication {

using ::dsn::error_code;
using ::dsn::service::zauto_write_lock;

class app_state;
class duplication_info
{
public:
    /// \see meta_duplication_service::new_dup_from_init
    duplication_info(dupid_t dupid,
                     int32_t appid,
                     std::string remote_cluster_address,
                     std::string meta_store_path)
        : id(dupid),
          app_id(appid),
          remote(std::move(remote_cluster_address)),
          store_path(std::move(meta_store_path)),
          create_timestamp_ms(dsn_now_ms())
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
        zauto_write_lock l(_lock);
        _is_altering = true;
        next_status = duplication_status::DS_START;
    }

    // change current status to `to`.
    // error will be returned if this state transition is not allowed.
    // Thread-Safe
    error_code alter_status(duplication_status::type to)
    {
        zauto_write_lock l(_lock);
        return do_alter_status(to);
    }

    // stable current status to `next_status`
    // call this function after data has been persisted on meta-state storage.
    // Thread-Safe
    void stable_status()
    {
        zauto_write_lock l(_lock);

        dassert(_is_altering, "");
        _is_altering = false;
        status = next_status;
        next_status = duplication_status::DS_INIT;
    }

    ///
    /// alter_progress -> stable_progress
    ///

    // Returns: false if the progress did not advance.
    // Thread-safe
    bool alter_progress(int partition_index, decree d)
    {
        zauto_write_lock l(_lock);

        if (_is_altering) {
            return false;
        }

        if (progress[partition_index] < d) {
            progress[partition_index] = d;
        }
        if (progress[partition_index] != stored_progress[partition_index]) {
            // progress update is not supposed to be too frequent.
            if (dsn_now_ms() > last_progress_update + PROGRESS_UPDATE_PERIOD_MS) {
                _is_altering = true;
                last_progress_update = dsn_now_ms();
                return true;
            }
        }
        return false;
    }

    // Thread-Safe
    void stable_progress()
    {
        zauto_write_lock l(_lock);

        dassert(_is_altering, "");
        _is_altering = false;
        stored_progress = std::move(progress);
    }

    // This function should only be used for testing.
    // Not-Thread-Safe
    bool is_altering() const { return _is_altering; }

    // Thread-Safe
    bool equals_to(const duplication_info &rhs) const { return to_string() == rhs.to_string(); }

    // Thread-Safe
    std::string to_string() const
    {
        blob b = to_json_blob();
        return std::string(b.data(), b.length());
    }

    // Thread-Safe
    std::shared_ptr<duplication_info> copy() const { return create_from_blob(to_json_blob()); }

    // Thread-Safe
    std::shared_ptr<duplication_info> copy_in_status(duplication_status::type status) const
    {
        auto dup = copy();
        dup->status = status;
        return dup;
    }

    // Thread-Safe
    blob to_json_blob() const
    {
        ::dsn::service::zauto_read_lock l(_lock);
        return json::json_forwarder<duplication_info>::encode(*this);
    }

    // Acquire lock for duplication_info.
    // This is a dangerous action, using the thread-safe methods is recommended.
    ::dsn::service::zrwlock_nr &lock_unsafe() { return _lock; }

    const dupid_t id{0};
    const int32_t app_id{0};
    const std::string remote;
    const std::string store_path; // store path on meta service = get_duplication_path(app, dupid)
    const uint64_t create_timestamp_ms{0}; // the time when this dup is created.

private:
    friend class duplication_info_test;

    duplication_info() = default;

    error_code do_alter_status(duplication_status::type to);

    // Whether the state is changing.
    // To ensure that there's only one task updating this duplication.
    bool _is_altering{false};

    mutable ::dsn::service::zrwlock_nr _lock;

    static constexpr int PROGRESS_UPDATE_PERIOD_MS = 5000;

public:
    // The following fields are made public to be accessible for
    // json decoder. It should be noted that they are not thread-safe
    // for user.

    duplication_status::type status{duplication_status::DS_INIT};
    duplication_status::type next_status{duplication_status::DS_INIT};

    // partition index -> the decree that's been replicated to remote
    std::map<int, int64_t> progress;
    // the latest progress that's been persisted in meta-state storage
    std::map<int, int64_t> stored_progress;
    // the time of last progress update to meta-state storage
    uint64_t last_progress_update{0};

    DEFINE_JSON_SERIALIZATION(id, remote, status, create_timestamp_ms, progress);
};

typedef std::shared_ptr<duplication_info> duplication_info_s_ptr;

} // namespace replication
} // namespace dsn

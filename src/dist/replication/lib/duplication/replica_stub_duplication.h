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

#include <atomic>

#include "dist/replication/lib/replica_stub.h"

#include <dsn/dist/replication/duplication_common.h>
#include <dsn/utility/chrono_literals.h>
#include <dsn/dist/replication/duplication_backlog_handler.h>

namespace dsn {
namespace replication {

using namespace dsn::literals::chrono_literals;

class replica_stub::duplication_impl
{
public:
    explicit duplication_impl(replica_stub *stub)
        : _stub(stub),
          _duplication_sync_interval_ms(
              std::chrono::milliseconds(_stub->options().duplication_sync_interval_ms))
    {
        ddebug_f("start duplication sync every {}ms", _duplication_sync_interval_ms.count());
    }

    ~duplication_impl()
    {
        _paused = true;
        dsn_task_tracker_wait_all(tracker()->tracker());
    }

    void initialize_and_start()
    {
        duplication_backlog_handler_factory::initialize();
        enqueue_duplication_sync_timer();
    }

    /// ================================= Implementation ============================= ///

    // always running in a single task
    void enqueue_duplication_sync_timer(std::chrono::milliseconds delay_ms = 0_ms)
    {
        if (_paused) {
            return;
        }

        tasking::enqueue(
            LPC_DUPLICATION_SYNC_TIMER, tracker(), [this]() { duplication_sync(); }, 0, delay_ms);
    }

    // replica server periodically uploads current confirm points to meta server by sending
    // `duplication_sync_request`.
    // if success, meta server will respond with `duplication_sync_response`, which contains
    // the current set of duplications.
    void duplication_sync();

    void on_duplication_sync_reply(error_code err, duplication_sync_rpc rpc);

    void call_duplication_sync_rpc(std::unique_ptr<duplication_sync_request> req);

    void update_duplication_map(const std::map<int32_t, std::vector<duplication_entry>> &dup_map);

    clientlet *tracker() { return _stub; }

private:
    friend class replica_stub_duplication_test;

    replica_stub *_stub;
    std::atomic_bool _paused{false};

    const std::chrono::milliseconds _duplication_sync_interval_ms{10_s};
};

} // namespace replication
} // namespace dsn

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

#include "mutation_duplicator.h"

#include <dsn/cpp/message_utils.h>

#include "dist/replication/lib/mutation_log_utils.h"
#include "dist/replication/lib/prepare_list.h"
#include "dist/replication/lib/mutation_log.h"
#include "dist/replication/lib/replica.h"
#include "mutation_batch.h"

namespace dsn {
namespace replication {

// Loads mutations from private log into memory.
// It works in THREAD_POOL_REPLICATION_LONG (LPC_DUPLICATION_LOAD_MUTATIONS),
// which permits tasks to be executed in a blocking way.
class private_log_loader
{
public:
    explicit private_log_loader(mutation_duplicator *duplicator) {}

    /// Read a block of mutations starting from decree `d` into mutation_batch.
    /// This function doesn't block.
    void load_mutations_from_decree(decree d)
    {
        _paused = false;
        _start_decree = d;

        if (_current_log_file == nullptr) {
            enqueue_start_loading_from_decree(d);
        } else {
            enqueue_do_load_mutations();
        }
    }

    void pause() { _paused = true; }

    /// ================================= Implementation =================================== ///

    gpid get_gpid() { return; }

    void enqueue_start_loading_from_decree(decree d, std::chrono::milliseconds delay_ms = 0_ms)
    {
        if (_paused) {
            return;
        }

        tasking::enqueue(LPC_DUPLICATION_LOAD_MUTATIONS,
                         tracker(),
                         std::bind(&private_log_loader::start_loading_from_decree, this, d),
                         0,
                         delay_ms);
    }

    void start_loading_from_decree(decree d)
    {
        std::vector<std::string> log_files = log_utils::list_all_files_or_die(_private_log->dir());
        _current_log_file = find_log_file_containing_decree(log_files, get_gpid(), d);
        if (_current_log_file == nullptr) {
            // wait 10 seconds if no log available.
            enqueue_start_loading_from_decree(d, 10_s);
            return;
        }

        _current_global_end_offset = _current_log_file->start_offset();

        do_load_mutations();
    }

    // Find the log file that contains decree `d`.
    // RETURNS: null if there's no valid log file.
    static log_file_ptr
    find_log_file_containing_decree(const std::vector<std::string> &log_files, gpid id, decree d);

    void enqueue_do_load_mutations(std::chrono::milliseconds delay_ms = 0_ms)
    {
        if (_paused) {
            return;
        }

        tasking::enqueue(LPC_DUPLICATION_LOAD_MUTATIONS,
                         tracker(),
                         std::bind(&private_log_loader::do_load_mutations, this),
                         0,
                         delay_ms);
    }

    void do_load_mutations();

    // Switches to the log file with index = current_log_index + 1.
    void switch_to_next_log_file();

    error_s replay_log_block()
    {
        return mutation_log::replay_block(
            _current_log_file,
            [this](int log_length, mutation_ptr &mu) -> bool {
                auto es = _mutation_batch.add(std::move(mu));
                if (!es.is_ok()) {
                    dfatal_replica("invalid mutation was found. err: {}", es.description());
                }
                return true;
            },
            _read_from_start,
            _current_global_end_offset);
    }

    clientlet *tracker() { return; }

private:
    std::atomic<bool> _paused{true};

    int64_t _current_global_end_offset{0};
    log_file_ptr _current_log_file;
    bool _read_from_start{true};
    decree _start_decree{0};
    mutation_batch _mutation_batch;

    mutation_log_ptr _private_log;
};

} // namespace replication
} // namespace dsn

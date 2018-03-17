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

namespace dsn {
namespace replication {

// A sorted array of committed mutations which are ready for duplication.
struct mutation_batch
{
    static const int64_t prepare_list_num_entries = 200;

    mutation_batch()
        : _mutation_buffer(0,
                           prepare_list_num_entries,
                           [](mutation_ptr &) {
                               // do nothing when log commit
                           }),
          _last_decree(0)
    {
    }

    error_s add(mutation_ptr mu);

    // After calling this function this `batch` is guaranteed to be empty.
    mutation_tuple_set move_to_mutation_tuples() { return std::move(_mutations); }

    decree last_decree() const { return _last_decree; }

    bool empty() const { return _mutations.empty(); }

    /// ================================= Implementation =================================== ///

    void add_mutation_tuple_if_valid(mutation_update &update, uint64_t timestamp)
    {
        // ignore WRITE_EMPTY (heartbeat)
        if (update.code == RPC_REPLICATION_WRITE_EMPTY) {
            return;
        }
        dsn_message_t req = from_blob_to_received_msg(
            update.code, update.data, 0, 0, dsn_msg_serialize_format(update.serialization_type));
        _mutations.emplace(std::make_tuple(timestamp, req, std::move(update.data)));
    }

    friend class mutation_duplicator_test;

    prepare_list _mutation_buffer;
    mutation_tuple_set _mutations;
    decree _last_decree;
};

using mutation_batch_u_ptr = std::unique_ptr<mutation_batch>;

// Loads mutations from private log into memory.
// It works in THREAD_POOL_REPLICATION_LONG (LPC_DUPLICATION_LOAD_MUTATIONS),
// which permits tasks to be executed in a blocking way.
class mutation_loader
{
public:
    explicit mutation_loader(mutation_duplicator *duplicator)
        : _duplicator(duplicator), _private_log(duplicator->_replica->private_log())
    {
    }

    /// Read a block of mutations into mutation_batch,
    /// once success it will ship them through mutation_duplicator.
    /// \see mutation_duplicator::do_duplicate
    void load_mutations()
    {
        _paused = false;
        enqueue_start_loading(_short_delay_in_ms);
    }

    void pause()
    {
        // need to reload when restart.
        _paused = true;
    }

    /// ================================= Implementation =================================== ///

    gpid get_gpid() { return _duplicator->get_gpid(); }

    void enqueue_start_loading(std::chrono::milliseconds delay_ms = 0_ms)
    {
        tasking::enqueue(LPC_DUPLICATION_LOAD_MUTATIONS,
                         tracker(),
                         std::bind(&mutation_loader::start_loading, this),
                         0,
                         delay_ms);
    }

    void start_loading()
    {
        if (_paused) {
            return;
        }

        std::vector<std::string> log_files = log_utils::list_all_files_or_die(_private_log->dir());

        // start duplication from the first log file.
        _current_log_file = log_utils::find_log_file_with_min_index(log_files);
        if (_current_log_file == nullptr) {
            // wait 10 seconds if no log available.
            enqueue_start_loading(10_s);
            return;
        }

        do_load_mutations();
    }

    void enqueue_do_load_mutations(std::chrono::milliseconds delay_ms = 0_ms)
    {
        tasking::enqueue(LPC_DUPLICATION_LOAD_MUTATIONS,
                         tracker(),
                         std::bind(&mutation_loader::do_load_mutations, this),
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
            _current_end_offset);
    }

    clientlet *tracker() { return _duplicator->tracker(); }

private:
    std::atomic<bool> _paused{true};

    int64_t _current_end_offset{0};
    log_file_ptr _current_log_file;
    int64_t _current_log_file_size{0};
    bool _read_from_start{true};

    mutation_log_ptr _private_log;
    mutation_batch _mutation_batch;
    mutation_duplicator *_duplicator;

    // mutation_loader will take a long break (long_delay_in_ms) for loading error
    // and small log block (normally a small block indicates no writes incoming recently).
    // In pegasus if
    // If everything works well, it only takes a short break every time it loads up
    // a log block.
    const std::chrono::milliseconds _long_delay_in_ms{0_ms};
    const std::chrono::milliseconds _short_delay_in_ms{0_ms};
};

} // namespace replication
} // namespace dsn

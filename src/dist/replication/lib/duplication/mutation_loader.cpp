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

#include "mutation_loader.h"

namespace dsn {
namespace replication {

error_s mutation_batch::add(mutation_ptr mu)
{
    error_code ec = _mutation_buffer.prepare(mu, partition_status::PS_INACTIVE);
    if (ec != ERR_OK) {
        return FMT_ERR(ERR_INVALID_DATA,
                       "mutation_batch: failed to add mutation [err: {}, mutation decree: "
                       "{}, ballot: {}]",
                       ec,
                       mu->get_decree(),
                       mu->get_ballot());
    }

    while (true) {
        mutation_ptr popped = _mutation_buffer.pop_min();
        if (popped == nullptr) {
            break;
        }

        if (popped->get_decree() <= _mutation_buffer.last_committed_decree()) {
            for (mutation_update &update : popped->data.updates) {
                add_mutation_tuple_if_valid(update, popped->data.header.timestamp);
            }

            // update last_decree
            _last_decree = std::max(_last_decree, popped->get_decree());
        } else {
            _mutation_buffer.prepare(popped, partition_status::PS_INACTIVE);
            break;
        }
    }

    dassert(_mutation_buffer.count() < prepare_list_num_entries,
            "impossible! prepare_list has reached the capacity");
    return error_s::ok();
}

/*extern*/ log_file_ptr
find_log_file_containing_decree(const std::vector<std::string> &log_files, gpid id, decree d)
{
    std::map<int, log_file_ptr> log_file_map = log_utils::open_log_file_map(log_files);
    if (log_file_map.empty()) {
        return nullptr;
    }

    dassert_f(log_file_map.begin()->second->previous_log_max_decree(id) < d,
              "log file containing decree({}) may have been compacted",
              d);

    for (auto it = log_file_map.begin(); it != log_file_map.end(); it++) {
        auto next_it = std::next(it);
        if (next_it == log_file_map.end()) {
            return it->second;
        }
        if (it->second->previous_log_max_decree(id) < d &&
            d <= next_it->second->previous_log_max_decree(id)) {
            return it->second;
        }
    }

    __builtin_unreachable();
}

void mutation_loader::do_load_mutations()
{
    if (_paused) {
        return;
    }

    error_s err = replay_log_block();
    if (!err.is_ok()) {
        // EOF appears only when end of log file is reached.
        if (err.code() == ERR_HANDLE_EOF) {
            switch_to_next_log_file();
            return;
        }

        dwarn_replica("error occurred while loading mutation logs: [err: {}, file: {}]",
                      err,
                      _current_log_file->path());

        // reload infinitely if error
        _read_from_start = true;
        enqueue_do_load_mutations(_long_delay_in_ms);
        return;
    }

    _read_from_start = false;

    // There're two cases when file size doesn't increase.
    //  1. no writes for now
    //  2. new log file is created
    // On either cases we wait for (2 * group_check + flush), in which there must have
    // at least one log block (WRITE_EMPTY) flushed into file.
    // Under this precondition it's guaranteed that whenever EOF occurred, the writing
    // file is switched.
    if (_current_log_file_size == _current_end_offset) {
        utils::filesystem::file_size(_current_log_file->path(), _current_log_file_size);
        if (_current_log_file_size == _current_end_offset) {
            // there's no progress
            enqueue_do_load_mutations(_long_delay_in_ms);
            return;
        }
    }

    if (_mutation_batch.empty()) {
        enqueue_do_load_mutations(_short_delay_in_ms);
        return;
    }

    _duplicator->_last_prepared_decree = _mutation_batch.last_decree();
    _duplicator->_pending_mutations = _mutation_batch.move_to_mutation_tuples();
    _duplicator->enqueue_ship_mutations();
}

void mutation_loader::switch_to_next_log_file()
{
    std::string new_path = fmt::format(
        "{}/log.{}.{}", _private_log->dir(), _current_log_file->index() + 1, _current_end_offset);

    if (utils::filesystem::file_exists(new_path)) {
        _current_log_file = log_utils::open_read_or_die(new_path);
        _read_from_start = true;

        ddebug_replica("switched log file to: {}", new_path);
        enqueue_do_load_mutations(_short_delay_in_ms);
    } else {
        tasking::enqueue(LPC_DUPLICATION_LOAD_MUTATIONS,
                         tracker(),
                         std::bind(&mutation_loader::switch_to_next_log_file, this),
                         0,
                         _long_delay_in_ms);
    }
}

} // namespace replication
} // namespace dsn

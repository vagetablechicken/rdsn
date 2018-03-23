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

#include "private_log_loader.h"

namespace dsn {
namespace replication {

/*static*/ log_file_ptr private_log_loader::find_log_file_containing_decree(
    const std::vector<std::string> &log_files, gpid id, decree d)
{
    std::map<int, log_file_ptr> log_file_map = log_utils::open_log_file_map(log_files);
    if (log_file_map.empty()) {
        return nullptr;
    }

    if (d == 0) {
        // start from first log file if it's a new duplication.
        return log_file_map.begin()->second;
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

void private_log_loader::do_load_mutations()
{
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

    if (_mutation_batch.empty()) {
        enqueue_do_load_mutations(_short_delay_in_ms);
        return;
    }

    _retriever->_last_prepared_decree = _mutation_batch.last_decree();
    _retriever->_pending_mutations = _mutation_batch.move_to_mutation_tuples();
    _retriever->enqueue_ship_mutations();
}

void private_log_loader::switch_to_next_log_file()
{
    std::string new_path = fmt::format("{}/log.{}.{}",
                                       _private_log->dir(),
                                       _current_log_file->index() + 1,
                                       _current_global_end_offset);

    if (utils::filesystem::file_exists(new_path)) {
        _current_log_file = log_utils::open_read_or_die(new_path);
        _read_from_start = true;

        ddebug_replica("switched log file to: {}", new_path);
        enqueue_do_load_mutations(_short_delay_in_ms);
    } else {
        tasking::enqueue(LPC_DUPLICATION_LOAD_MUTATIONS,
                         tracker(),
                         std::bind(&private_log_loader::switch_to_next_log_file, this),
                         0,
                         _long_delay_in_ms);
    }
}

} // namespace replication
} // namespace dsn

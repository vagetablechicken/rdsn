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

void load_from_private_log::switch_to_next_log_file()
{
    std::string new_path = fmt::format(
        "{}/log.{}.{}", _private_log->dir(), _current->index() + 1, _current_global_end_offset);

    if (utils::filesystem::file_exists(new_path)) {
        _current = log_utils::open_read_or_die(new_path);
        _read_from_start = true;

        ddebug_replica("switched log file to: {}", new_path);
    }
}

void load_from_private_log::run()
{
    if (_current == nullptr) {
        std::vector<std::string> log_files = log_utils::list_all_files_or_die(_private_log->dir());
        find_log_file_to_start(log_files);
        if (_current == nullptr) {
            // wait 10 seconds if no log available.
            repeat(10_s);
            return;
        }
        _current_global_end_offset = _current->start_offset();
    } else if (_next != nullptr && _next->previous_log_max_decree(_gpid) < _start_decree) {
        // change the working file.
        _current = nullptr;
        _next = nullptr;
        repeat();
        return;
    }

    load_from_log_file();
}

void load_from_private_log::find_log_file_to_start(const std::vector<std::string> &log_files)
{
    decree d = _start_decree;

    std::map<int, log_file_ptr> log_file_map = log_utils::open_log_file_map(log_files);
    if (log_file_map.empty()) {
        // no file.
        return;
    }

    auto begin = log_file_map.begin();

    if (d == 0) { // start from first log file if it's a new duplication.
        _current = begin->second;
        if (log_file_map.size() > 1) {
            _next = std::next(begin)->second;
        }
        return;
    }

    dassert_f(begin->second->previous_log_max_decree(get_gpid()) < d,
              "log file containing decree({}) may have been compacted",
              d);

    for (auto it = begin; it != log_file_map.end(); it++) {
        auto next_it = std::next(it);
        if (next_it == log_file_map.end()) {
            // use the last file
            _current = it->second;
            return;
        }
        if (it->second->previous_log_max_decree(get_gpid()) < d &&
            d <= next_it->second->previous_log_max_decree(get_gpid())) {
            _current = it->second;
            _next = next_it->second;
            return;
        }
    }

    __builtin_unreachable();
}

void load_from_private_log::load_from_log_file()
{
    error_s err = replay_log_block();
    if (!err.is_ok()) {
        // EOF appears only when end of log file is reached.
        if (err.code() == ERR_HANDLE_EOF) {
            switch_to_next_log_file();
            repeat(10_s);
            return;
        }

        dwarn_replica("error occurred while loading mutation logs: [err: {}, file: {}]",
                      err,
                      _current->path());

        // reload infinitely if error
        _read_from_start = true;
        repeat(10_s);
        return;
    }

    _read_from_start = false;

    if (_mutation_batch.empty()) {
        repeat(10_s);
    } else {
        step_down_next_stage(_mutation_batch.move_all_mutations());
    }
}

} // namespace replication
} // namespace dsn

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

#include <fmt/format.h>

#include "dist/replication/lib/mutation_log.h"

namespace dsn {
namespace replication {

/*static*/ error_code mutation_log::replay(log_file_ptr log,
                                           replay_callback callback,
                                           /*out*/ int64_t &end_offset)
{
    ddebug("start to replay mutation log %s, offset = [%" PRId64 ", %" PRId64 "), size = %" PRId64,
           log->path().c_str(),
           log->start_offset(),
           log->end_offset(),
           log->end_offset() - log->start_offset());

    error_s err;
    bool start = true;
    while (true) {
        err = replay_block(log, callback, start, end_offset);
        if (!err.is_ok()) {
            break;
        }

        start = false;
    }

    ddebug("finish to replay mutation log (%s) [err: %s]",
           log->path().c_str(),
           err.description().c_str());
    return err.code();
}

namespace internal {

inline static error_s read_log_block(log_file_ptr &log,
                                     int64_t &end_offset,
                                     std::unique_ptr<binary_reader> &reader,
                                     blob &bb)
{
    error_code err = log->read_next_log_block(bb);
    if (err != ERR_OK) {
        return error_s::make(err, "failed to read log block");
    }
    reader = dsn::make_unique<binary_reader>(bb);
    end_offset += sizeof(log_block_header);

    return error_s::ok();
}

} // namespace internal

/*static*/ error_s mutation_log::replay_block(log_file_ptr log,
                                              replay_callback callback,
                                              bool read_from_start,
                                              int64_t &end_offset)
{
    blob bb;
    std::unique_ptr<binary_reader> reader;

    if (read_from_start) {
        end_offset = log->start_offset();
        log->reset_stream();
    }

    error_s err = internal::read_log_block(log, end_offset, reader, bb);
    if (!err.is_ok()) {
        return err;
    }

    if (read_from_start) {
        end_offset += log->read_file_header(*reader);
        if (!log->is_right_header()) {
            return error_s::make(ERR_INVALID_DATA, "failed to read log file header");
        }
    }

    while (!reader->is_eof()) {
        auto old_size = reader->get_remaining_size();
        mutation_ptr mu = mutation::read_from(*reader, nullptr);
        dassert(nullptr != mu, "");
        mu->set_logged();

        if (mu->data.header.log_offset != end_offset) {
            return error_s::make(ERR_INVALID_DATA,
                                 fmt::format("offset mismatch in log entry and mutation {} vs {}",
                                             end_offset,
                                             mu->data.header.log_offset));
        }

        int log_length = old_size - reader->get_remaining_size();
        callback(log_length, mu);

        end_offset += log_length;
    }

    return error_s::ok();
}

/*static*/ error_code mutation_log::replay(std::vector<std::string> &log_files,
                                           replay_callback callback,
                                           /*out*/ int64_t &end_offset)
{
    std::map<int, log_file_ptr> logs;
    for (auto &fpath : log_files) {
        error_code err;
        log_file_ptr log = log_file::open_read(fpath.c_str(), err);
        if (log == nullptr) {
            if (err == ERR_HANDLE_EOF || err == ERR_INCOMPLETE_DATA ||
                err == ERR_INVALID_PARAMETERS) {
                dinfo("skip file %s during log replay", fpath.c_str());
                continue;
            } else {
                return err;
            }
        }

        dassert(
            logs.find(log->index()) == logs.end(), "invalid log_index, index = %d", log->index());
        logs[log->index()] = log;
    }

    return replay(logs, callback, end_offset);
}

/*static*/ error_code mutation_log::replay(std::map<int, log_file_ptr> &logs,
                                           replay_callback callback,
                                           /*out*/ int64_t &end_offset)
{
    int64_t g_start_offset = 0;
    int64_t g_end_offset = 0;
    error_code err = ERR_OK;
    log_file_ptr last;
    int last_file_index = 0;

    if (logs.size() > 0) {
        g_start_offset = logs.begin()->second->start_offset();
        g_end_offset = logs.rbegin()->second->end_offset();
        last_file_index = logs.begin()->first - 1;
    }

    // check file index continuity
    for (auto &kv : logs) {
        if (++last_file_index != kv.first) {
            derror("log file missing with index %u", last_file_index);
            return ERR_OBJECT_NOT_FOUND;
        }
    }

    end_offset = g_start_offset;

    for (auto &kv : logs) {
        log_file_ptr &log = kv.second;

        if (log->start_offset() != end_offset) {
            derror("offset mismatch in log file offset and global offset %" PRId64 " vs %" PRId64,
                   log->start_offset(),
                   end_offset);
            return ERR_INVALID_DATA;
        }

        last = log;
        err = mutation_log::replay(log, callback, end_offset);

        log->close();

        if (err == ERR_OK || err == ERR_HANDLE_EOF) {
            // do nothing
        } else if (err == ERR_INCOMPLETE_DATA) {
            // If the file is not corrupted, it may also return the value of ERR_INCOMPLETE_DATA.
            // In this case, the correctness is relying on the check of start_offset.
            dwarn("delay handling error: %s", err.to_string());
        } else {
            // for other errors, we should break
            break;
        }
    }

    if (err == ERR_OK || err == ERR_HANDLE_EOF) {
        // the log may still be written when used for learning
        dassert(g_end_offset <= end_offset,
                "make sure the global end offset is correct: %" PRId64 " vs %" PRId64,
                g_end_offset,
                end_offset);
        err = ERR_OK;
    } else if (err == ERR_INCOMPLETE_DATA) {
        // ignore the last incomplate block
        err = ERR_OK;
    } else {
        // bad error
        derror("replay mutation log failed: %s", err.to_string());
    }

    return err;
}

} // namespace replication
} // namespace dsn

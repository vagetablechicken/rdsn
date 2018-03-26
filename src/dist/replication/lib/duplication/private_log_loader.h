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
#include "mutation_batch.h"

#include <dsn/cpp/message_utils.h>
#include <dsn/cpp/pipeline.h>

#include "dist/replication/lib/mutation_log_utils.h"
#include "dist/replication/lib/prepare_list.h"
#include "dist/replication/lib/mutation_log.h"
#include "dist/replication/lib/replica.h"

namespace dsn {
namespace replication {

class private_log_loader : pipeline::base
{
    private_log_loader() { from(_load.get()).link(); }

public:
    std::unique_ptr<load_from_private_log> _load;
};

/// Loads mutations from private log into memory.
/// It works in THREAD_POOL_REPLICATION_LONG (LPC_DUPLICATION_LOAD_MUTATIONS),
/// which permits tasks to be executed in a blocking way.
class load_from_private_log : pipeline::when_arg<decree>, pipeline::result<mutation_tuple_set>
{
    void run() override;

    /// =================================== Implementation =================================== ///

    /// Find the log file that contains decree `d`.
    void find_log_file_to_start(const std::vector<std::string> &log_files);

    void load_from_log_file();

    gpid get_gpid() { return _gpid; }

    error_s replay_log_block()
    {
        return mutation_log::replay_block(
            _current,
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

    // Switches to the log file with index = current_log_index + 1.
    void switch_to_next_log_file();

    explicit load_from_private_log(mutation_duplicator *duplicator) : _gpid(duplicator->get_gpid())
    {
    }

private:
    mutation_log *_private_log;
    gpid _gpid;

    log_file_ptr _current, _next;
    bool _read_from_start{true};
    int64_t _current_global_end_offset{0};
    mutation_batch _mutation_batch;

    decree _start_decree{0};
};

} // namespace replication
} // namespace dsn

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

#include <dsn/cpp/clientlet.h>
#include <dsn/dist/replication/fmt_logging.h>
#include <dsn/dist/replication/replication_types.h>
#include <dsn/dist/replication/replication_app_base.h>

#include "dist/replication/lib/replica.h"
#include "dist/replication/lib/prepare_list.h"
#include "dist/replication/lib/replica.h"
#include "dist/replication/lib/mutation_log.h"

#include "duplication_view.h"

namespace dsn {
namespace replication {

// Each mutation_duplicator is responsible for one duplication.
class mutation_duplicator
{
public:
    struct mutation_batch
    {
        static const int64_t per_batch_size_bytes = 1 * 1024 * 1024; // 1MB
        static const int64_t per_batch_num_entries = per_batch_size_bytes / 64;

        explicit mutation_batch(int64_t init_decree)
            : _total_size(0), _mutations(init_decree, per_batch_num_entries, [](mutation_ptr &) {
                  // do nothing when log commit
              })
        {
        }

        error_s add(int log_length, mutation_ptr mu)
        {
            RETURN_NOT_OK(_mutations.check_if_valid_to_prepare(mu));

            _mutations.prepare(mu, partition_status::PS_INACTIVE);
            _total_size += log_length;

            return error_s::ok();
        }

        bool oversize() const
        {
            return _total_size > per_batch_size_bytes ||
                   _mutations.count() >= per_batch_num_entries;
        }

    private:
        friend class mutation_duplicator;
        friend class mutation_duplicator_test;

        int64_t _total_size;
        prepare_list _mutations;
    };

    using mutation_batch_u_ptr = std::unique_ptr<mutation_batch>;

public:
    mutation_duplicator(const duplication_entry &ent, replica *r)
        : _replica(r),
          _paused(true),
          _private_log(r->private_log()),
          _last_duplicate_time(0),
          _tracker(1)
    {
        if (_replica->last_durable_decree() > ent.confirmed_decree) {
            dfatal_f("the logs haven't yet duplicated were accidentally truncated [replica: "
                     "(appid:{}, pid:{}), last_durable_decree: {}, confirmed_decree: {}]",
                     _replica->get_gpid().get_app_id(),
                     _replica->get_gpid().get_partition_index(),
                     _replica->last_durable_decree(),
                     ent.confirmed_decree);
        }

        _view = make_unique<duplication_view>(ent.dupid, ent.remote_address);
        _view->confirmed_decree = ent.confirmed_decree;
        _view->status = ent.status;
        _view->last_decree = ent.confirmed_decree;
    }

    ~mutation_duplicator()
    {
        pause();
        dsn_task_tracker_wait_all(tracker()->tracker());
    }

    void start_duplication()
    {
        dassert(_paused, "start an already started mutation_duplicator");

        _paused = false;
        tasking::enqueue(LPC_DUPLICATE_MUTATIONS,
                         tracker(),
                         std::bind(&mutation_duplicator::do_duplicate, this),
                         gpid_to_thread_hash(_replica->get_gpid()));
    }

    void pause()
    {
        if (!_paused) {
            _paused = true;
        }
    }

    void do_duplicate()
    {
        if (_paused) {
            return;
        }

        if (_mutation_batches.empty()) {
            bool no_mutation = load_mutations();
            if (_paused) { // check again
                return;
            }
            if (no_mutation) {
                // wait for 5 seconds for next try if no mutation was added.
                tasking::enqueue(LPC_DUPLICATE_MUTATIONS,
                                 tracker(),
                                 std::bind(&mutation_duplicator::do_duplicate, this),
                                 gpid_to_thread_hash(_replica->get_gpid()),
                                 std::chrono::milliseconds(5000));
            }
        }

        ship_mutations();

        // delay 1 second to start next duplication job
        tasking::enqueue(LPC_DUPLICATE_MUTATIONS,
                         tracker(),
                         std::bind(&mutation_duplicator::do_duplicate, this),
                         gpid_to_thread_hash(_replica->get_gpid()),
                         std::chrono::milliseconds(1000));
    }

    // RETURNS: false if failed to load mutation logs.
    bool load_mutations()
    {
        if (_view->last_decree >= _private_log->_private_log_info.max_decree) {
            return false;
        }
        log_file_ptr prev_log_file;
        for (auto &e : _private_log->_log_files) {
            log_file_ptr log_file = e.second;

            decree d = log_file->previous_log_max_decree(_replica->get_gpid());
            if (d <= _view->last_decree) {
                prev_log_file = log_file;
            } else {
                return load_mutations_from_log_file(prev_log_file);
            }
        }
        return false;
    }

    // REQUIRES: `log_file` must have at least one mutation.
    // RETURNS: false if error occurred.
    bool load_mutations_from_log_file(log_file_ptr &log_file)
    {
        int64_t end_offset = 0;
        error_code ec = mutation_log::replay(
            log_file,
            [this](int log_length, mutation_ptr &mu) -> bool {
                if (_mutation_batches.empty()) {
                    _mutation_batches.emplace_back(make_unique<mutation_batch>(0));
                }

                mutation_batch &back_batch = *_mutation_batches.back();
                if (back_batch.oversize()) {
                    int64_t next_init_decree = back_batch._mutations.last_committed_decree() + 1;
                    _mutation_batches.emplace_back(make_unique<mutation_batch>(next_init_decree));
                }

                mutation_batch &batch = *_mutation_batches.back();
                auto es = batch.add(log_length, std::move(mu));
                if (!es.is_ok()) {
                    ddebug_f("ignore this invalid mutation: {}", es.description());
                    return true;
                }
                return true;
            },
            end_offset);

        if (ec != ERR_OK && ec != ERR_HANDLE_EOF) {
            dwarn_f("error occurred while loading mutation logs: [ec: {}, file: {}]",
                    ec.to_string(),
                    log_file->path());
            return false;
        }
        return true;
    }

    // RETURNS: false if no more mutations to ship, true otherwise.
    bool ship_mutations()
    {
        if (_mutation_batches.empty()) {
            return false;
        }

        auto backlog_handler = _replica->get_app()->get_duplication_backlog_handler();
        _last_duplicate_time = dsn_now_ms();

        std::vector<dsn_message_t> mutations;
        mutation_batch_to_vec_message(_mutation_batches.front().get(), &mutations);

        backlog_handler->duplicate(&mutations);
        _mutation_batches.pop_front();

        return true;
    }

    const duplication_view &view() const { return *_view; }

    duplication_view *mutable_view() { return _view.get(); }

    static void mutation_batch_to_vec_message(mutation_batch *batch,
                                              std::vector<dsn_message_t> *dest)
    {
        prepare_list &mutations = batch->_mutations;
        while (true) {
            auto mu = mutations.pop_min();
            if (mu == nullptr) {
                break;
            }

            for (const mutation_update &update : mu->data.updates) {
                dsn_message_t req = dsn_msg_create_received_request(
                    update.code,
                    (dsn_msg_serialize_format)update.serialization_type,
                    (void *)update.data.data(),
                    update.data.length());

                dest->push_back(req);
            }
        }
    }

    // Returns: the task tracker.
    clientlet *tracker() { return &_tracker; }

private:
    friend class mutation_duplicator_test;

    replica *_replica;

    std::atomic<bool> _paused;

    std::deque<mutation_batch_u_ptr> _mutation_batches;

    mutation_log_ptr _private_log;

    // in milliseconds
    int64_t _last_duplicate_time;

    duplication_view_u_ptr _view;

    clientlet _tracker;
};

typedef std::shared_ptr<mutation_duplicator> mutation_duplicator_s_ptr;

} // namespace replication
} // namespace dsn

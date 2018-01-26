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
#include "dist/replication/lib/mutation_log_utils.h"
#include "dist/replication/lib/duplication/duplication_view.h"
#include "dist/replication/lib/duplication/fmt_utils.h"

namespace dsn {
namespace replication {

// Each mutation_duplicator is responsible for one duplication.
class mutation_duplicator
{
    struct mutation_batch
    {
        // TODO(wutao1): loads configuration log_private_batch_buffer_count to
        // initialize this value.
        static const int64_t per_batch_num_entries = 1024;

        explicit mutation_batch(int64_t init_decree)
            : _mutations(init_decree, per_batch_num_entries, [](mutation_ptr &) {
                  // do nothing when log commit
              })
        {
        }

        error_s add(int log_length, mutation_ptr mu)
        {
            RETURN_NOT_OK(_mutations.check_if_valid_to_prepare(mu));

            _mutations.prepare(mu, partition_status::PS_INACTIVE);

            return error_s::ok();
        }

        bool empty() const { return _mutations.count() == 0; }

        friend class mutation_duplicator_test;

        prepare_list _mutations;
    };

    using mutation_batch_u_ptr = std::unique_ptr<mutation_batch>;

public:
    mutation_duplicator(const duplication_entry &ent, replica *r)
        : _replica(r),
          _private_log(r->private_log()),
          _paused(true),
          _mutation_batch(make_unique<mutation_batch>(0)),
          _current_end_offset(0),
          _read_from_start(true),
          _last_duplicate_time_ms(0),
          _tracker(1)
    {
        if (_replica->last_durable_decree() > ent.confirmed_decree) {
            dfatal_f("the logs haven't yet duplicated were accidentally truncated [replica: "
                     "(gpid: {}), last_durable_decree: {}, confirmed_decree: {}]",
                     _replica->get_gpid(),
                     _replica->last_durable_decree(),
                     ent.confirmed_decree);
        }

        _view = make_unique<duplication_view>(ent.dupid, ent.remote_address);
        _view->confirmed_decree = ent.confirmed_decree;
        _view->status = ent.status;

        // start from the last confirmed decree
        _view->last_decree = ent.confirmed_decree;
    }

    ~mutation_duplicator()
    {
        pause();
        dsn_task_tracker_wait_all(tracker()->tracker());
    }

    void
    enqueue_start_duplication(std::chrono::milliseconds delay_ms = std::chrono::milliseconds(0))
    {
        tasking::enqueue(LPC_DUPLICATE_MUTATIONS,
                         tracker(),
                         std::bind(&mutation_duplicator::start_duplication, this),
                         gpid_to_thread_hash(_replica->get_gpid()),
                         delay_ms);
    }

    void start_duplication()
    {
        dassert(_paused, "start an already started mutation_duplicator");
        _paused = false;

        std::vector<std::string> log_files = log_utils::list_all_files_or_die(_private_log->dir());

        // start duplication from the first log file.
        _current_log_file = find_log_file_with_min_index(log_files);
        if (_current_log_file == nullptr) {
            // wait 10 seconds if there's no private log.
            enqueue_start_duplication(std::chrono::milliseconds(1000 * 10));
            return;
        }

        enqueue_do_duplication();
    }

    void pause()
    {
        if (!_paused) {
            _paused = true;
        }
    }

    const duplication_view &view() const { return *_view; }

    duplication_view *mutable_view() { return _view.get(); }

    // Returns: the task tracker.
    clientlet *tracker() { return &_tracker; }

    // Await for all running tasks to complete.
    void wait_all() { dsn_task_tracker_wait_all(tracker()->tracker()); }

    /// ================================= Implementation =================================== ///

    // RETURNS: null if there's no valid log file.
    static log_file_ptr find_log_file_with_min_index(const std::vector<std::string> &log_files)
    {
        std::map<int, log_file_ptr> log_file_map = log_utils::open_log_file_map(log_files);
        if (log_file_map.empty()) {
            return nullptr;
        }
        return log_file_map.begin()->second;
    }

    void enqueue_do_duplication(std::chrono::milliseconds delay_ms = std::chrono::milliseconds(0))
    {
        tasking::enqueue(LPC_DUPLICATE_MUTATIONS,
                         tracker(),
                         std::bind(&mutation_duplicator::do_duplicate, this),
                         gpid_to_thread_hash(_replica->get_gpid()),
                         delay_ms);
    }

    void do_duplicate()
    {
        if (_paused) {
            return;
        }

        if (_mutation_batch->empty()) {
            if (!have_more()) {
                // wait 10 seconds for next try if no mutation was added.
                enqueue_do_duplication(std::chrono::milliseconds(1000 * 10));
                return;
            }

            _mutation_batch = make_unique<mutation_batch>(
                _mutation_batch->_mutations.last_committed_decree() + 1);

            if (!load_mutations_from_log_file(_current_log_file)) {
                if (try_switch_to_next_log_file()) {
                    // read another log file.
                    enqueue_do_duplication();
                } else {
                    // wait 10 sec if there're mutations written but unreadable.
                    enqueue_do_duplication(std::chrono::milliseconds(1000 * 10));
                }
                return;
            }

            if (_paused) { // check again
                return;
            }
        }

        if (_mutation_batch->empty()) {
            // retry if the loaded block contains no mutations (typically the header block)
            enqueue_do_duplication(std::chrono::milliseconds(1000));
        } else {
            ship_loaded_mutation_batch();
        }
    }

    task_ptr ship_loaded_mutation_batch()
    {
        _last_duplicate_time_ms = dsn_now_ms();
        std::vector<dsn_message_t> mutations;
        mutation_batch_to_vec_message(_mutation_batch.get(), &mutations);
        return enqueue_ship_mutations(mutations);
    }

    // Switches to the log file with index = current_log_index + 1.
    bool try_switch_to_next_log_file()
    {
        std::string new_path = fmt::format("{}/log-{}-{}",
                                           _private_log->dir(),
                                           _current_log_file->index() + 1,
                                           _current_end_offset);

        bool result = false;
        if (utils::filesystem::file_exists(new_path)) {
            result = true;
        } else {
            // TODO(wutao1): edge case handling
        }

        if (result) {
            _current_log_file = log_utils::open_read_or_die(new_path);
            _read_from_start = true;
        }
        return result;
    }

    bool have_more() const { return _private_log->max_commit_on_disk() > _view->last_decree; }

    // REQUIRES: `log_file` must have at least one mutation.
    // RETURNS: false if nothing was loaded.
    bool load_mutations_from_log_file(log_file_ptr &log_file)
    {
        error_s err = mutation_log::replay_block(
            log_file,
            [this](int log_length, mutation_ptr &mu) -> bool {
                auto es = _mutation_batch->add(log_length, std::move(mu));
                if (!es.is_ok()) {
                    ddebug_f("ignore this invalid mutation: {}", es.description());
                    return true;
                }
                return true;
            },
            _read_from_start,
            _current_end_offset);

        if (!err.is_ok()) {
            if (err.code() != ERR_HANDLE_EOF) {
                dwarn_f("error occurred while loading mutation logs: [err: {}, file: {}]",
                        err,
                        log_file->path());
            }
            return false;
        } else {
            _read_from_start = false;
        }
        return true;
    }

    task_ptr
    enqueue_ship_mutations(std::vector<dsn_message_t> &mutations,
                           std::chrono::milliseconds delay_ms = std::chrono::milliseconds(0))
    {
        return tasking::enqueue(
            LPC_DUPLICATE_MUTATIONS,
            tracker(),
            std::bind(&mutation_duplicator::ship_mutations, this, std::move(mutations)),
            gpid_to_thread_hash(_replica->get_gpid()),
            delay_ms);
    }

    void ship_mutations(std::vector<dsn_message_t> &mutations)
    {
        auto backlog_handler = _replica->get_app()->get_duplication_backlog_handler();
        error_s err = backlog_handler->duplicate(&mutations);
        if (!err.is_ok()) {
            derror_f("failed to ship mutations [gpid: {}]: {}", _replica->get_gpid(), err);

            // retries forever until all mutations are duplicated.
            enqueue_ship_mutations(mutations);
        } else {
            // delay 1 second to start next duplication job
            enqueue_do_duplication(std::chrono::milliseconds(1000));
        }
    }

    // After calling this function the `batch` is guaranteed to be empty.
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

private:
    friend class mutation_duplicator_test;

    replica *_replica;
    mutation_log_ptr _private_log;

    std::atomic<bool> _paused;

    std::unique_ptr<mutation_batch> _mutation_batch;

    int64_t _current_end_offset;
    log_file_ptr _current_log_file;
    bool _read_from_start;

    int64_t _last_duplicate_time_ms;

    duplication_view_u_ptr _view;

    clientlet _tracker;
};

typedef std::shared_ptr<mutation_duplicator> mutation_duplicator_s_ptr;

} // namespace replication
} // namespace dsn
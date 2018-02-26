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

#include <gtest/gtest.h>
#include <dsn/dist/meta_state_service.h>
#include <dsn/dist/replication/replication.codes.h>

#include "dist/replication/meta_server/meta_options.h"

using dsn::dist::meta_state_service;
using dsn::replication::meta_options;

namespace dsn {

class meta_state_service_test : public ::testing::Test
{
public:
    meta_state_service_test() : _tracker(1) {}

    ~meta_state_service_test() override {}

    void SetUp() override
    {
        meta_options meta_opts;
        meta_opts.initialize();

        dinfo("meta_state_service_type: %s", meta_opts.meta_state_service_type.c_str());

        _storage = ::dsn::utils::factory_store<meta_state_service>::create(
            meta_opts.meta_state_service_type.c_str(), dsn::PROVIDER_TYPE_MAIN);
        _storage->initialize({});
    }

    void TearDown() override { delete _storage; }

    void delete_node(const std::string &path)
    {
        _storage->delete_node(path,
                              true,
                              LPC_META_STATE_HIGH,
                              [path, this](error_code ec) {
                                  if (ec != ERR_OK)
                                      ddebug("%s", ec.to_string());
                              },
                              &_tracker);
        dsn_task_tracker_wait_all(_tracker.tracker());
    }

protected:
    meta_state_service *_storage;
    clientlet _tracker;
};

} // namespace dsn

using namespace dsn;

TEST_F(meta_state_service_test, create_and_delete)
{
    std::string path = "/meta_state_service_test";

    delete_node(path);
    _storage->create_node(path,
                          LPC_META_STATE_HIGH,
                          [path, this](error_code ec) {
                              ASSERT_EQ(ec, ERR_OK) << " create_node";
                              _storage->delete_node(path,
                                                    true,
                                                    LPC_META_STATE_HIGH,
                                                    [this](error_code ec) {
                                                        ASSERT_EQ(ec, ERR_OK) << " delete_node";
                                                    },
                                                    &_tracker);
                          },
                          blob("hello", 0, 5),
                          &_tracker);
    dsn_task_tracker_wait_all(_tracker.tracker());
}

// Create a created node will get ERR_NODE_ALREADY_EXIST.
TEST_F(meta_state_service_test, create_duplicated_node)
{
    std::string path = "/meta_state_service_test";

    delete_node(path);

    _storage->create_node(path,
                          LPC_META_STATE_HIGH,
                          [path, this](error_code ec) {
                              ASSERT_EQ(ec, ERR_OK) << " create_node1";
                              _storage->create_node(path,
                                                    LPC_META_STATE_HIGH,
                                                    [this](error_code ec) {
                                                        ASSERT_EQ(ec, ERR_NODE_ALREADY_EXIST)
                                                            << " create_node2";
                                                    },
                                                    blob("hello", 0, 5),
                                                    &_tracker);
                          },
                          blob("hello", 0, 5),
                          &_tracker);
    dsn_task_tracker_wait_all(_tracker.tracker());
}

// Ensure that attempts to access non-existed node will get ERR_OBJECT_NOT_FOUND.
TEST_F(meta_state_service_test, non_existed_node)
{
    std::string path = "/meta_state_service_test";

    delete_node(path);

    _storage->delete_node(
        path,
        true,
        LPC_META_STATE_HIGH,
        [this](error_code ec) { ASSERT_EQ(ec, ERR_OBJECT_NOT_FOUND) << " delete_node"; },
        &_tracker);
    _storage->set_data(
        path,
        blob("***", 0, 3),
        LPC_META_STATE_HIGH,
        [this](error_code ec) { ASSERT_EQ(ec, ERR_OBJECT_NOT_FOUND) << " set_data"; },
        &_tracker);
    _storage->get_data(path,
                       LPC_META_STATE_HIGH,
                       [this](error_code ec, const blob &val) {
                           ASSERT_EQ(ec, ERR_OBJECT_NOT_FOUND) << " get_data";
                       },
                       &_tracker);
    dsn_task_tracker_wait_all(_tracker.tracker());
}

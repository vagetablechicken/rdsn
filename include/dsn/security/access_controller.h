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

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <bitset>
#include <memory>
#include <initializer_list>

namespace dsn {
namespace security {

enum class acl_bit
{
    // A,
    // C,
    W,
    R,
    // X
}; // string RW

class access_controller
{
public:
    access_controller();
    bool pre_check(std::string rpc_code, std::string user_name);

    // bool need_app_check() { return _need_app_check; }
    // bool app_level_check(std::string rpc_code,
    //                      std::string user_name,
    //                      const std::map<std::string, std::string> &app_acl);
    bool app_level_check(std::string rpc_code,
                         std::string user_name,
                         const std::string &acl_entries_str);

    // bool cache_check(std::string rpc_code, std::string user_name, int app_id);

    void
    load_config(const std::string &super_user, const bool open_auth, const bool mandatory_auth);

    // void update_cache(int app_id, std::map<std::string, std::string> upsert_entries);
    void update_cache(int app_id, const std::string &upsert_entries_str);

    bool bit_check(const int app_id, const std::string &user_name, const acl_bit bit);
    bool is_superuser(const std::string &user_name) { return _super_user == user_name; }

private:
    void register_entries(std::initializer_list<std::string> il, std::string mask)
    {
        for (auto rpc_code : il) {
            _acl_masks[rpc_code] = std::bitset<10>(mask);
        }
    }
    void register_entry(std::string &rpc_code, std::string &mask)
    {
        _acl_masks[rpc_code] = std::bitset<10>(mask);
    }
    void register_allpass_entries(std::initializer_list<std::string> il)
    {
        for (auto rpc_code : il)
            _all_pass.insert(rpc_code);
    }

    bool cluster_level_check(std::string rpc_code, std::string user_name);

    std::string _super_user;
    bool _open_auth;
    bool _mandatory_auth;

    std::unordered_map<std::string, std::bitset<10>> _acl_masks;
    std::unordered_set<std::string> _all_pass;

    std::unordered_map<int, std::unordered_map<std::string, std::string>> _cached_app_acls;
    // boost::shared_mutex _mutex; // for _cached_app_acls
};
}
}
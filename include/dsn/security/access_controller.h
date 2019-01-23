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

#include <atomic>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <bitset>
#include <memory>
#include <initializer_list>

#include <dsn/c/api_utilities.h>

namespace dsn {
namespace security {
// access_controller only support RW for normal users, so
// meta side: only pre_check
// replica side: only bit_check
enum class acl_bit
{
    // A,
    // C,
    W,
    R,
    // X
}; // string RW

typedef std::unordered_map<int, std::unordered_map<std::string, std::string>> acls_map;
class rcu_map
{
public:
    rcu_map() : _m0(std::make_shared<acls_map>()), _m1(std::make_shared<acls_map>()) {}

    std::shared_ptr<acls_map> dereference()
    {
        if (_read0.load())
            return _m0;
        return _m1;
    }

    acls_map::iterator find(int app_id)
    {
        if (_read0.load())
            return _m0->find(app_id);
        return _m1->find(app_id);
    }
    acls_map::iterator end()
    {
        if (_read0.load())
            return _m0->end();
        return _m1->end();
    }
    void update(std::shared_ptr<acls_map> &&temp)
    {
        dassert(temp.unique(), "temp is referenced by others");
        std::shared_ptr<acls_map> reload_map = get_reload_map();

        // reload_map reloads the new acls (it means the grace period is over)
        // old acls will be released automatically, cause it has been managed by shared_ptr after
        // the swap
        reload_map->swap(*temp);

        swith_read();
    }

private:
    std::shared_ptr<acls_map> get_reload_map()
    {
        if (_read0.load()) {
            return _m1;
        }
        return _m0;
    }
    void swith_read()
    {
        bool b = _read0.load();
        _read0.store(!b);
    }
    std::atomic_bool _read0{true}; // true: read m0; false: read m1
    std::shared_ptr<acls_map> _m0, _m1;
};

class access_controller
{
public:
    static const std::string ACL_KEY;
    static void decode_and_insert(int app_id,
                                  const std::string &acl_entries_str,
                                  std::shared_ptr<acls_map> acls);

    access_controller();

    void
    load_config(const std::string &super_user, const bool open_auth, const bool mandatory_auth);
    bool is_superuser(const std::string &user_name)
    {
        return !_open_auth || !_mandatory_auth || _super_user == user_name;
    }

    // for meta
    bool pre_check(const std::string &rpc_code, const std::string &user_name);
    bool cluster_level_check(const std::string &rpc_code,
                             const std::string &user_name); // not implemented
    bool app_level_check(const std::string &rpc_code,
                         const std::string &user_name,
                         const std::string &acl_entries_str);

    // for replica, only check RW bit
    bool bit_check(const int app_id, const std::string &user_name, const acl_bit bit);
    void update_cache(std::shared_ptr<acls_map> &&temp)
    {
        _cached_app_acls.update(std::move(temp));
    }

private:
    void register_entries(std::initializer_list<std::string> il, const std::string &mask)
    {
        for (auto rpc_code : il) {
            _acl_masks[rpc_code] = std::bitset<10>(mask);
        }
    }
    void register_entry(const std::string &rpc_code, const std::string &mask)
    {
        _acl_masks[rpc_code] = std::bitset<10>(mask);
    }
    void register_allpass_entries(std::initializer_list<std::string> il)
    {
        for (auto rpc_code : il) {
            _all_pass.insert(rpc_code);
        }
    }

    std::string _super_user;
    bool _open_auth;
    bool _mandatory_auth;

    std::unordered_map<std::string, std::bitset<10>> _acl_masks;
    std::unordered_set<std::string> _all_pass;

    rcu_map _cached_app_acls;
};
}
}
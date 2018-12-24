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
#include <dsn/security/access_controller.h>
#include <dsn/c/api_utilities.h>
#include "dist/replication/meta_server/meta_data.h"

namespace dsn {
namespace security {

access_controller::access_controller()
{
    // initial rpc permission template

    // 1.rpc_rrdb
    register_entries({"RPC_RRDB_RRDB_GET",
                      "RPC_RRDB_RRDB_MULTI_GET",
                      "RPC_RRDB_RRDB_SORTKEY_COUNT",
                      "RPC_RRDB_RRDB_TTL",
                      "RPC_RRDB_RRDB_GET_SCANNER",
                      "RPC_RRDB_RRDB_SCAN",
                      "RPC_RRDB_RRDB_CLEAR_SCANNER"},
                     "10");

    register_entries({"RPC_RRDB_RRDB_PUT",
                      "RPC_RRDB_RRDB_MULTI_PUT",
                      "RPC_RRDB_RRDB_REMOVE",
                      "RPC_RRDB_RRDB_MULTI_REMOVE",
                      "RPC_RRDB_RRDB_INCR"},
                     "01");

    register_entries({"RPC_RRDB_RRDB_CHECK_AND_SET", "RPC_RRDB_RRDB_CHECK_AND_MUTATE"}, "11");

    // 2. meta
    register_allpass_entries({"RPC_CM_LIST_APPS",
                              "RPC_CM_LIST_NODES",
                              "RPC_CM_CLUSTER_INFO",
                              "RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX"});
    // RPC_CM_QUERY_NODE_PARTITIONS, )
    // RPC_CM_CONFIG_SYNC, )
    // RPC_CM_UPDATE_PARTITION_CONFIGURATION, )
    // RPC_CM_CREATE_APP, )
    // RPC_CM_DROP_APP, )
    // RPC_CM_RECALL_APP, )
    // RPC_CM_CONTROL_META, )
    // RPC_CM_START_RECOVERY, )
    // RPC_CM_START_RESTORE, )

    // RPC_CM_PROPOSE_BALANCER, )
    // RPC_CM_ADD_BACKUP_POLICY, )
    // RPC_CM_QUERY_BACKUP_POLICY, )
    // RPC_CM_MODIFY_BACKUP_POLICY, )

    // RPC_CM_REPORT_RESTORE_STATUS, )
    // RPC_CM_QUERY_RESTORE_STATUS, )
    // RPC_CM_ADD_DUPLICATION, )
    // RPC_CM_CHANGE_DUPLICATION_STATUS, )
    // RPC_CM_QUERY_DUPLICATION, )
    // RPC_CM_DUPLICATION_SYNC, )
    // RPC_CM_UPDATE_APP_ENV, )
    // RPC_CM_DDD_DIAGNOSE, )
    //// RPC_CM_ACL_CONTROL, )
}

void access_controller::load_config(const std::string &super_user,
                                    const bool open_auth,
                                    const bool mandatory_auth)
{
    ddebug("load superuser(%s), open_auth(%d), mandatory_auth(%d)",
           super_user.c_str(),
           open_auth,
           mandatory_auth);
    _super_user = super_user;
    _open_auth = open_auth;
    _mandatory_auth = mandatory_auth;
}

bool access_controller::pre_check(std::string rpc_code, std::string user_name)
{
    if (!_open_auth || !_mandatory_auth || user_name == _super_user)
        return true;

    if (_all_pass.find(rpc_code) != _all_pass.end())
        return true;

    return false;
}

bool access_controller::cluster_level_check(std::string rpc_code, std::string user_name)
{
    // can't do cluster level check when using app_envs' acl
    return false;
}

// bool access_controller::app_level_check(std::string rpc_code,
//                                         std::string user_name,
//                                         const std::map<std::string, std::string> &app_acl)
// {
//    // boost::shared_lock<boost::shared_mutex> sl(_mutex);
//     if (_acl_masks.find(rpc_code) == _acl_masks.end()) {
//         ddebug("rpc_code %s is not registered", rpc_code.c_str());
//         return false;
//     }

//     if (app_acl.find(user_name) == app_acl.end()) {
//         ddebug("user_name %s doesn't exist in app_acl", user_name.c_str());
//         return false;
//     }

//     auto mask = _acl_masks[rpc_code];
//     auto permission =
//         std::bitset<10>(app_acl.find(user_name)->second); // TODO HW only accept binary strings
//         now

//     if ((permission & mask) == mask)
//         return true;

//     return false;
// }

bool access_controller::app_level_check(std::string rpc_code,
                                        std::string user_name,
                                        const std::string &acl_entries_str)
{
    auto mask_iter = _acl_masks.find(rpc_code);
    if (mask_iter == _acl_masks.end()) {
        ddebug("rpc_code %s is not registered", rpc_code.c_str());
        return false;
    }
    auto &mask = mask_iter->second;

    auto user_pos = std::string::npos;
    if ((user_pos = acl_entries_str.find(user_name)) == std::string::npos) {
        ddebug("user_name %s doesn't exist in acl_entries_str", user_name.c_str());
        return false;
    }
    auto end = acl_entries_str.find(";", user_pos);
    auto permission_pos = user_pos + user_name.size() + 1;
    std::string permission_str = acl_entries_str.substr(permission_pos, end - permission_pos);
    auto permission = std::bitset<10>(permission_str); // TODO HW only accept binary strings now

    if ((permission & mask) == mask)
        return true;

    return false;
}

// bool access_controller::cache_check(std::string rpc_code, std::string user_name, int app_id)
// {
//     ddebug("cache acl check rpc %s, user_name %s", rpc_code.c_str(), user_name.c_str());

//     if (pre_check(rpc_code, user_name))
//         return true;
//    // boost::shared_lock<boost::shared_mutex> sl(_mutex);
//     if (_cached_app_acls.find(app_id) == _cached_app_acls.end()) {
//         dwarn("app_acl(id %d) does not exist ", app_id);
//         return false;
//     }

//     if (app_level_check(rpc_code, user_name, _cached_app_acls[app_id]))
//         return true;
//     ddebug("acl deny");

//     return false;
// }

// void access_controller::update_cache(int app_id, std::map<std::string, std::string>
// upsert_entries)
// {
//     auto &app_acl = _cached_app_acls[app_id];
//     for (auto &item : upsert_entries) {
//         if (std::all_of(item.second.begin(), item.second.end(), [](char c) { return c == '0'; }))
//         {
//             app_acl.erase(item.first);
//         } else {
//             app_acl[item.first] = item.second;
//         }
//     }
// }

void access_controller::update_cache(int app_id, const std::string &acl_entries_str)
{
    std::unordered_map<std::string, std::string> app_acl;
    std::istringstream iss(acl_entries_str);
    std::string user_name, permission;
    while (getline(iss, user_name, ':')) {
        getline(iss, permission, ';');
        if (std::all_of(permission.begin(), permission.end(), [](char c) { return c == '0'; })) {
            // del
            app_acl.erase(app_acl.find(user_name));
        } else {
            // set
            app_acl[user_name] = permission;
        }
    }
    // boost::unique_lock<boost::shared_mutex> ul(_mutex);
    _cached_app_acls[app_id] = app_acl;
    ddebug("current cache:");
    for (auto &pair : _cached_app_acls[app_id]) {
        ddebug("%s %s", pair.first.c_str(), pair.second.c_str());
    }
}

bool access_controller::bit_check(const int app_id, const std::string &user_name, const acl_bit bit)
{
    if (!_open_auth || !_mandatory_auth || user_name == _super_user)
        return true;
    // boost::shared_lock<boost::shared_mutex> sl(_mutex);
    if (_cached_app_acls.find(app_id) == _cached_app_acls.end()) {
        ddebug("app_acl(id %d) does not exist ", app_id);
        return false;
    }

    auto app_acl = _cached_app_acls[app_id];
    auto entry = app_acl.find(user_name);
    if (entry == app_acl.end()) {
        ddebug("user_name %s doesn't exist in app_acl", user_name.c_str());
        return false;
    }
    auto permission = entry->second;
    return std::bitset<10>(permission)[static_cast<int>(bit)];
}
}
}
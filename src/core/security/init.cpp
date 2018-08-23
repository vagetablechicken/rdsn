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

#include <krb5/krb5.h>
#include <fmt/format.h>

#include <dsn/security/init.h>
#include <dsn/c/api_utilities.h>
#include <dsn/utility/config_api.h>
#include <dsn/utility/filesystem.h>
#include <dsn/utility/defer.h>
#include <dsn/utility/utils.h>

#include <functional>
#include <boost/asio/deadline_timer.hpp>

#include "core/tools/common/shared_io_service.h"

namespace dsn {
namespace security {

namespace {

class kinit_context;

static std::unique_ptr<utils::rw_lock_nr> g_kerberos_lock;
static std::unique_ptr<kinit_context> g_kinit_ctx;
static krb5_context g_krb5_context;
static std::string username; // parse from principal

class kinit_context
{
public:
    kinit_context() : _opt(nullptr) {}
    virtual ~kinit_context();
    // implementation of 'kinit -k -t <keytab_file> <principal>'
    error_s kinit(const std::string &keytab_file, const std::string &principal);

    std::string username() { return _username_str; }

    std::string service_fqdn() { return _service_fqdn; }

    std::string service_name() { return _service_name; }

private:
    // krb5 structure
    krb5_principal _principal;
    krb5_keytab _keytab; // absolute path
    krb5_ccache _ccache;
    krb5_get_init_creds_opt *_opt;

    // principal and username that logged in as, this determines "who I am"
    std::string _principal_name;
    std::string _username_str;

    // used for determine the remote service, this determines "who will I visit"
    std::string _service_fqdn;
    std::string _service_name;

    int64_t _cred_expire_timestamp;

    std::shared_ptr<boost::asio::deadline_timer> _timer;

    // get or renew credentials from KDC and store it to _ccache
    error_s get_formatted_identities();
    error_s get_credentials();
    void schedule_renew_credentials();
};

void init_krb5_ctx()
{
    static std::once_flag once;
    std::call_once(once, [&]() {
        int64_t err = krb5_init_context(&g_krb5_context);
        if (err != 0) {
            dassert(false,
                    "init kerberos context failed, with kerberos  error_code = %" PRId64 "",
                    err);
        }
    });
}

#undef KRB5_RETURN_NOT_OK
// please notice err may be an expression/function, but not a variable
#define KRB5_RETURN_NOT_OK(err, msg)                                                               \
    do {                                                                                           \
        krb5_error_code __err_code__ = (err);                                                      \
        if (__err_code__ != 0) {                                                                   \
            return krb5_call_to_errors(g_krb5_context, __err_code__, (msg));                       \
        }                                                                                          \
    } while (0);

#undef WRAP_KRB5_ERR
// please notice krb5_err may be an expression/function, but not a variable
#define WRAP_KRB5_ERR(krb5_err, result_err, msg)                                                   \
    do {                                                                                           \
        krb5_error_code __err_code__ = (krb5_err);                                                 \
        if (__err_code__ != 0) {                                                                   \
            result_err = krb5_call_to_errors(g_krb5_context, __err_code__, (msg));                 \
        } else {                                                                                   \
            result_err = error_s::ok();                                                            \
        }                                                                                          \
    } while (0)

// switch the code of krb5_xxx function to error_s
static error_s krb5_call_to_errors(krb5_context ctx, krb5_error_code code, const char *prefix_msg)
{
    std::unique_ptr<const char, std::function<void(const char *)>> error_msg(
        krb5_get_error_message(ctx, code),
        std::bind(krb5_free_error_message, ctx, std::placeholders::_1));

    std::string msg;
    if (prefix_msg != nullptr) {
        msg = prefix_msg;
        msg += ": ";
    }
    msg += error_msg.get();
    return error_s::make(ERR_RUNTIME_ERROR, msg.c_str());
}

static error_s parse_username_from_principal(krb5_const_principal principal, std::string &username)
{
    // Attention: here we just assume the length of username must be little then 1024
    char buf[1024];
    krb5_error_code err = 0;
    err = krb5_aname_to_localname(g_krb5_context, principal, sizeof(buf), buf);

    if (err == KRB5_LNAME_NOTRANS) {
        if (principal->length > 0) {
            int cnt = 0;
            while (cnt < principal->length) {
                std::string tname;
                tname.assign((const char *)principal->data[cnt].data, principal->data[cnt].length);
                if (!username.empty()) {
                    username += '/';
                }
                username += tname;
                cnt++;
            }
            return error_s::make(ERR_OK);
        }
        return error_s::make(ERR_RUNTIME_ERROR, "parse username from principal failed");
    }

    if (err == KRB5_CONFIG_NOTENUFSPACE) {
        return error_s::make(ERR_RUNTIME_ERROR, "username is larger than 1024");
    }

    KRB5_RETURN_NOT_OK(err, "krb5 parse aname to localname failed");

    if (strlen(buf) <= 0) {
        return error_s::make(ERR_RUNTIME_ERROR, "empty username");
    }
    username.assign((const char *)buf);
    return error_s::make(ERR_OK);
}

static std::string from_unix_seconds(int64_t unix_seconds)
{
    char buffer[128];
    utils::time_ms_to_date_time(unix_seconds * 1000, buffer, 128);
    return std::string(buffer);
}

static int64_t current_unix_seconds() { return utils::get_current_physical_time_ns() / 1000000000; }

// inline implementation of kinit_context
kinit_context::~kinit_context() { krb5_get_init_creds_opt_free(g_krb5_context, _opt); }

void kinit_context::schedule_renew_credentials()
{
    // reserve 300 seconds for renew
    int64_t renew_gap = _cred_expire_timestamp - current_unix_seconds() - 300;
    if (renew_gap < 300)
        renew_gap = 300;
    ddebug("schedule to renew credentials in %d seconds later", renew_gap);

    // why don't we use timers in rDSN framework?
    //  1. currently the rdsn framework may not started yet.
    //  2. the rdsn framework is used for codes of a service_app,
    //     not for codes under service_app
    if (!_timer)
        _timer.reset(new boost::asio::deadline_timer(tools::shared_io_service::instance().ios));
    _timer->expires_from_now(boost::posix_time::seconds(renew_gap));
    _timer->async_wait([this](const boost::system::error_code &err) {
        if (!err) {
            error_s e = get_credentials();

            // what if the KDC fails?
            // in this case, the authentication layer is untrusted any more.
            // could we kill ourselves?
            // dassert(e.is_ok(), "renew credentials failed");
            schedule_renew_credentials();
        } else if (err == boost::system::errc::operation_canceled) {
            dwarn("the renew credentials timer is cancelled");
        } else {
            dassert(false, "unhandled error(%s)", err.message().c_str());
        }
    });
}

error_s kinit_context::get_credentials()
{
    krb5_creds creds;
    error_s err = error_s::ok();

    WRAP_KRB5_ERR(krb5_get_init_creds_keytab(g_krb5_context,
                                             &creds,
                                             _principal,
                                             _keytab,
                                             0 /*valid from now*/,
                                             nullptr /*empty TKT service name*/,
                                             _opt),
                  err,
                  "get_init_cred");
    if (!err.is_ok()) {
        dwarn("get credentials of %s from KDC failed, reason(%s)",
              _principal_name.c_str(),
              err.description().c_str());
        return err;
    }
    auto cleanup = dsn::defer([&]() { krb5_free_cred_contents(g_krb5_context, &creds); });

    WRAP_KRB5_ERR(krb5_cc_store_cred(g_krb5_context, _ccache, &creds), err, "store_cred");
    if (!err.is_ok()) {
        dwarn("store credentials of %s to cache failed, err(%s)",
              _principal_name.c_str(),
              err.description().c_str());
        return err;
    }

    _cred_expire_timestamp = creds.times.endtime;
    ddebug("get credentials of %s from KDC ok, expires at %s",
           _principal_name.c_str(),
           from_unix_seconds(_cred_expire_timestamp).c_str());
    return err;
}

error_s kinit_context::get_formatted_identities()
{
    char *tmp_str = nullptr;
    KRB5_RETURN_NOT_OK(krb5_unparse_name(g_krb5_context, _principal, &tmp_str),
                       "unparse principal name failed");
    auto cleanup = dsn::defer([&]() { krb5_free_unparsed_name(g_krb5_context, tmp_str); });
    _principal_name = tmp_str;

    return parse_username_from_principal(_principal, _username_str);
}

error_s kinit_context::kinit(const std::string &keytab_file, const std::string &principal)
{
    if (keytab_file.empty() || principal.empty()) {
        return error_s::make(dsn::ERR_INVALID_PARAMETERS, "invalid keytab or principal");
    }

    init_krb5_ctx();

    KRB5_RETURN_NOT_OK(krb5_parse_name(g_krb5_context, principal.c_str(), &_principal),
                       "couldn't parse principal");

    RETURN_NOT_OK(get_formatted_identities());

    KRB5_RETURN_NOT_OK(krb5_kt_resolve(g_krb5_context, keytab_file.c_str(), &_keytab),
                       "couldn't resolve keytab file");

    KRB5_RETURN_NOT_OK(krb5_cc_default(g_krb5_context, &_ccache),
                       "couldn't acquire credential cache handle");

    KRB5_RETURN_NOT_OK(krb5_cc_initialize(g_krb5_context, _ccache, _principal),
                       "initialize credential cache failed");

    KRB5_RETURN_NOT_OK(krb5_get_init_creds_opt_alloc(g_krb5_context, &_opt),
                       "alloc get_init_creds_opt structure failed");

    RETURN_NOT_OK(get_credentials());

    schedule_renew_credentials();

    ddebug("logged in from keytab as %s, local username %s",
           _principal_name.c_str(),
           _username_str.c_str());

    _service_fqdn =
        dsn_config_get_value_string("security", "service_fqdn", "pegasus", "service fqdn");
    if (_service_fqdn.empty()) {
        return error_s::make(ERR_RUNTIME_ERROR, "invalid server fqdn");
    }
    _service_name =
        dsn_config_get_value_string("security", "service_name", "pegasus", "service name");
    if (_service_name.empty()) {
        return error_s::make(ERR_RUNTIME_ERROR, "invalid service name");
    }
    return error_s::make(ERR_OK);
}

#undef KRB5_RETURN_NOT_OK // only used in this anonymous namespace
#undef WRAP_KRB5_ERR      // only used in this anonymous namespace
} // end anonymous namespace

error_s init_kerberos(bool is_server)
{
    // acquire the keytab file from configuration
    std::string keytab_file =
        dsn_config_get_value_string("security", "krb5_keytab", "", "absolute path of keytab");
    if (keytab_file.empty() || !utils::filesystem::file_exists(keytab_file)) {
        return error_s::make(ERR_INVALID_PARAMETERS,
                             fmt::format("invalid keytab file \"{}\"", keytab_file));
    }

    std::string krb5_config =
        dsn_config_get_value_string("security", "krb5_config", "", "absolute path of krb5_config");
    if (krb5_config.empty() || !utils::filesystem::file_exists(krb5_config)) {
        return error_s::make(ERR_INVALID_PARAMETERS,
                             fmt::format("invalid krb5 config file \"{}\"", krb5_config));
    }

    std::string principal =
        dsn_config_get_value_string("security", "krb5_principal", "", "default principal");
    if (principal.empty()) {
        return error_s::make(ERR_INVALID_PARAMETERS, "empty principal");
    }

    // setup kerberos envs
    setenv("KRB5CCNAME", is_server ? "MEMORY:pegasus-server" : "MEMORY:pegasus-client", 1);
    setenv("KRB5_CONFIG", krb5_config.c_str(), 1);
    setenv("KRB5_KTNAME", keytab_file.c_str(), 1);
    setenv("KRB5RCACHETYPE", "none", 1);

    g_kinit_ctx.reset(new kinit_context);
    error_s err = g_kinit_ctx->kinit(keytab_file, principal);
    ddebug("after call kinit err = %s", err.description().c_str());

    g_kerberos_lock.reset(new utils::rw_lock_nr);
    // TODO: start a task to update the credential(TGT)
    return err;
}

utils::rw_lock_nr *krb5_cred_lock() { return g_kerberos_lock.get(); }

std::string get_username() { return g_kinit_ctx->username(); }

std::string get_service_fqdn() { return g_kinit_ctx->service_fqdn(); }

std::string get_service_name() { return g_kinit_ctx->service_name(); }

} // end namespace security
} // end namespace dsn

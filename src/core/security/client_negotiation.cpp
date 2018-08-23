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
#include <dsn/tool-api/rpc_address.h>
#include <dsn/security/init.h>
#include <dsn/security/client_negotiation.h>

namespace dsn {
namespace security {

// TODO: read expected mechanisms from config file
static const std::vector<std::string> expected_mechanisms{"GSSAPI"};

client_negotiation::client_negotiation(rpc_session *session)
    : _session(session), _user_name("unknown"), _status(negotiation_status::SASL_LIST_MECHANISMS)
{
    _name = fmt::format("C_NEGO_L({})=>R({})",
                        _session->local_address().to_string(),
                        _session->remote_address().to_string());
}

void client_negotiation::start_negotiate()
{
    ddebug("%s: start negotiation", _name.c_str());
    list_mechanisms();
}

void client_negotiation::send(const negotiation_message &n)
{
    _status = n.status;
    message_ptr msg = message_ex::create_request(RPC_NEGOTIATION);
    dsn::marshall(msg.get(), n);
    _session->send_message(msg.get());
}

void client_negotiation::fail_negotiation()
{
    _status = negotiation_status::SASL_AUTH_FAIL;
    _session->complete_negotiation(false);
}

void client_negotiation::succ_negotiation()
{
    _status = negotiation_status::SASL_SUCC;
    _session->complete_negotiation(true);
}

void client_negotiation::list_mechanisms()
{
    negotiation_message req;
    req.status = negotiation_status::SASL_LIST_MECHANISMS;
    send(req);
}

void client_negotiation::recv_mechanisms(const message_ptr &mechs_msg)
{
    negotiation_message resp;
    dsn::unmarshall(mechs_msg, resp);

    if (resp.status != negotiation_status::SASL_LIST_MECHANISMS_RESP) {
        dwarn("%s: got message(%s) while expect(%s), reason(%s)",
              _name.c_str(),
              enum_to_string(resp.status),
              enum_to_string(negotiation_status::SASL_LIST_MECHANISMS_RESP));
        fail_negotiation();
        return;
    }

    bool found_mechanisms = false;
    std::vector<std::string> supported_mechanisms;
    std::string resp_string = resp.msg.to_string();
    dsn::utils::split_args(resp_string.c_str(), supported_mechanisms, ',');

    for (const std::string &s : supported_mechanisms) {
        if (s == expected_mechanisms[0]) {
            ddebug("%s: found %s mech in server, use it",
                   _name.c_str(),
                   expected_mechanisms[0].c_str());
            found_mechanisms = true;
            break;
        }
    }

    if (!found_mechanisms) {
        dwarn("%s: server only support mechs of (%s), can't find expected (%s)",
              resp_string.c_str(),
              join(expected_mechanisms.begin(), expected_mechanisms.end(), ",").c_str());
        fail_negotiation();
        return;
    }

    select_mechanism(expected_mechanisms[0]);
}

void client_negotiation::select_mechanism(dsn::string_view mech)
{
    _selected_mechanism.assign(mech.data(), mech.length());

    negotiation_message req;
    req.status = negotiation_status::SASL_SELECT_MECHANISMS;
    req.msg = dsn::blob::create_from_bytes(mech.data(), mech.length());

    send(req);
}

void client_negotiation::mechanism_selected(const message_ptr &mechs_msg)
{
    negotiation_message resp;
    dsn::unmarshall(mechs_msg.get(), resp);
    if (resp.status == negotiation_status::SASL_SELECT_MECHANISMS_OK) {
        initiate_negotiation();
    } else {
        dwarn("%s: select mechanism(%s) from server failed, type(%s), reason(%s)",
              _name.c_str(),
              _selected_mechanism.c_str(),
              enum_to_string(resp.status),
              resp.msg.to_string().c_str());
        fail_negotiation();
    }
}

void client_negotiation::initiate_negotiation()
{
    error_s err_s = do_sasl_client_init();
    if (!err_s.is_ok()) {
        dassert(false,
                "%s: initiaze sasl client failed, error = %s, reason = %s",
                _name.c_str(),
                err_s.code().to_string(),
                err_s.description().c_str());
        fail_negotiation();
        return;
    }

    err_s = send_sasl_initiate_msg();

    error_code code = err_s.code();
    std::string desc = err_s.description();

    if (code == ERR_AUTH_FAILED && desc.find("Ticket expired") != std::string::npos) {
        derror("%s: start client negotiation with ticket expire, waiting on ticket renew",
               _name.c_str());
        fail_negotiation();
    } else if (code != ERR_OK && code != ERR_INCOMPLETE) {
        dassert(false,
                "%s: client_negotiation: send sasl_client_start failed, error = %s, reason = %s",
                _name.c_str(),
                code.to_string(),
                desc.c_str());
        fail_negotiation();
    }
}

error_s client_negotiation::do_sasl_client_init()
{
    sasl_conn_t *conn = nullptr;
    error_s err_s = call_sasl_func(nullptr, [&]() {
        return sasl_client_new(get_service_name().c_str(),
                               get_service_fqdn().c_str(),
                               nullptr,
                               nullptr,
                               nullptr,
                               0,
                               &conn);
    });

    if (err_s.is_ok()) {
        _sasl_conn.reset(conn);
    }

    return err_s;
}

error_s client_negotiation::send_sasl_initiate_msg()
{
    const char *msg = nullptr;
    unsigned msg_len = 0;
    const char *client_mech = nullptr;

    error_s err_s = call_sasl_func(_sasl_conn.get(), [&]() {
        return sasl_client_start(
            _sasl_conn.get(), _selected_mechanism.data(), nullptr, &msg, &msg_len, &client_mech);
    });

    error_code code = err_s.code();
    if (code == ERR_OK || code == ERR_INCOMPLETE) {
        dinfo("%s: call sasl_client_start succ with msg, len = %d", _name.c_str(), msg_len);
        negotiation_message req;
        req.status = negotiation_status::SASL_INITIATE;
        req.msg = dsn::blob::create_from_bytes(msg, msg_len);
        send(req);
    }

    return err_s;
}

error_s client_negotiation::retrive_user_name_from_sasl_conn(std::string &output)
{
    char *username = nullptr;
    error_s err_s = call_sasl_func(_sasl_conn.get(), [&]() {
        return sasl_getprop(_sasl_conn.get(), SASL_USERNAME, (const void **)&username);
    });

    if (err_s.is_ok()) {
        output = username;
        output = output.substr(0, output.find_last_of('@'));
        output = output.substr(0, output.find_first_of('/'));
    }
    return err_s;
}

error_s client_negotiation::do_sasl_step(const dsn::blob &input, blob &output)
{
    const char *msg = nullptr;
    unsigned msg_len = 0;
    error_s err_s = call_sasl_func(_sasl_conn.get(), [&]() {
        return sasl_client_step(
            _sasl_conn.get(), input.data(), input.length(), nullptr, &msg, &msg_len);
    });

    output = dsn::blob::create_from_bytes(msg, msg_len);
    return err_s;
}

void client_negotiation::handle_message_from_server(message_ptr msg)
{
    if (msg->error() == ERR_HANDLER_NOT_FOUND && !_session->mandantory_auth()) {
        dwarn("%s: treat negotiation succeed as server doesn't support it, user_name in later "
              "messages aren't trustable",
              _name.c_str());
        succ_negotiation();
        return;
    }
    if (msg->error() != ERR_OK) {
        derror("%s: negotiation failed, error = %s", _name.c_str(), msg->error().to_string());
        fail_negotiation();
        return;
    }
    if (_status == negotiation_status::SASL_LIST_MECHANISMS) {
        recv_mechanisms(msg);
        return;
    }
    if (_status == negotiation_status::SASL_SELECT_MECHANISMS) {
        mechanism_selected(msg);
        return;
    }
    handle_challenge(msg);
}

void client_negotiation::handle_challenge(const message_ptr &challenge_msg)
{
    negotiation_message challenge;
    dsn::unmarshall(challenge_msg, challenge);
    dinfo("%s: client recv negotiation message from server", _name.c_str());

    if (challenge.status == negotiation_status::type::SASL_AUTH_FAIL) {
        dwarn("%s: auth failed, reason(%s)", _name.c_str(), challenge.msg.to_string().c_str());
        fail_negotiation();
        return;
    }

    if (challenge.status == negotiation_status::type::SASL_CHALLENGE) {
        dsn::blob response_msg;
        error_s err_s = do_sasl_step(challenge.msg, response_msg);
        if (err_s.code() != ERR_OK && err_s.code() != ERR_INCOMPLETE) {
            derror("%s: negotiation failed locally, reason = %s",
                   _name.c_str(),
                   err_s.description().c_str());
            fail_negotiation();
            return;
        }

        negotiation_message resp;
        resp.status = negotiation_status::type::SASL_RESPONSE;
        resp.msg = response_msg;
        send(resp);
        return;
    }

    if (challenge.status == negotiation_status::type::SASL_SUCC) {
        ddebug("%s: negotiation succ", _name.c_str());
        error_s err = retrive_user_name_from_sasl_conn(_user_name);
        dassert(err.is_ok(),
                "%s: can't get user name for completed connection reason (%s)",
                _name.c_str(),
                err.description().c_str());
        succ_negotiation();
        return;
    }

    derror("%s: recv wrong negotiation msg, type = %s",
           _name.c_str(),
           enum_to_string(challenge.status));
    fail_negotiation();
}

} // end namespace security
} // end namespace dsn

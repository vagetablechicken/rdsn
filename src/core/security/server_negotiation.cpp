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
#include <dsn/security/server_negotiation.h>

namespace dsn {
namespace security {

static const std::vector<std::string> supported_mechanisms{"GSSAPI"};

server_negotiation::server_negotiation(rpc_session *session)
    : _session(session), _user_name("unknown"), _status(negotiation_status::SASL_LIST_MECHANISMS)
{
    _name = fmt::format("S_NEGO_L({})=>R({})",
                        _session->local_address().to_string(),
                        _session->remote_address().to_string());
}

void server_negotiation::start_negotiate() { ddebug("%s: start negotiation", _name.c_str()); }

void server_negotiation::reply(const message_ptr &req, const negotiation_message &response_data)
{
    _status = response_data.status;

    message_ptr resp = req->create_response();
    strncpy(resp->header->server.error_name,
            ERR_OK.to_string(),
            sizeof(resp->header->server.error_name));
    resp->header->server.error_code.local_code = ERR_OK; // rpc is ok
    resp->header->server.error_code.local_hash = message_ex::s_local_hash;
    dsn::marshall(resp, response_data);

    _session->send_message(resp);
}

void server_negotiation::fail_negotiation(const message_ptr &req, dsn::string_view reason)
{
    negotiation_message response;
    response.status = negotiation_status::SASL_AUTH_FAIL;
    response.msg = dsn::blob::create_from_bytes(reason.data(), reason.length());
    reply(req, response);

    _session->complete_negotiation(false);
}

void server_negotiation::succ_negotiation(const message_ptr &req)
{
    negotiation_message response;
    response.status = negotiation_status::SASL_SUCC;
    reply(req, response);

    _session->complete_negotiation(true);
}

void server_negotiation::on_list_mechanisms(const message_ptr &m)
{
    negotiation_message request;
    dsn::unmarshall(m, request);
    if (request.status == negotiation_status::SASL_LIST_MECHANISMS) {
        std::string mech_list = join(supported_mechanisms.begin(), supported_mechanisms.end(), ",");
        ddebug("%s: reply server mechs(%s)", _name.c_str(), mech_list.c_str());
        negotiation_message response;
        response.status = negotiation_status::SASL_LIST_MECHANISMS_RESP;
        response.msg = dsn::blob::create_from_bytes(std::move(mech_list));
        reply(m, response);
    } else {
        dwarn("%s: got message(%s) while expect(%s)",
              _name.c_str(),
              enum_to_string(request.status),
              negotiation_status::SASL_LIST_MECHANISMS);
        fail_negotiation(m, "invalid_client_message_status");
    }
}

void server_negotiation::on_select_mechanism(const message_ptr &m)
{
    negotiation_message request;
    dsn::unmarshall(m, request);
    if (request.status == negotiation_status::SASL_SELECT_MECHANISMS) {
        _selected_mechanism = request.msg.to_string();
        ddebug("%s: client select mechanism(%s)", _name.c_str(), _selected_mechanism.c_str());
        dassert(_selected_mechanism == "GSSAPI", "only gssapi supported");

        error_s err_s = do_sasl_server_init();
        if (!err_s.is_ok()) {
            dwarn("%s: server initialize sasl failed, error = %s, msg = %s",
                  _name.c_str(),
                  err_s.code().to_string(),
                  err_s.description().c_str());
            fail_negotiation(m, err_s.description());
            return;
        }

        negotiation_message response;
        response.status = negotiation_status::SASL_SELECT_MECHANISMS_OK;
        reply(m, response);
    } else {
        dwarn("%s: got message(%s) while expect(%s)",
              _name.c_str(),
              enum_to_string(request.status),
              negotiation_status::SASL_SELECT_MECHANISMS);
        fail_negotiation(m, "invalid_client_message_status");
    }
}

error_s server_negotiation::do_sasl_server_init()
{
    sasl_conn_t *conn = nullptr;
    error_s err_s = call_sasl_func(nullptr, [&]() {
        return sasl_server_new(get_service_name().c_str(),
                               get_service_fqdn().c_str(),
                               nullptr,
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

error_s server_negotiation::do_sasl_server_start(const blob &input, blob &output)
{
    const char *msg = nullptr;
    unsigned msg_len = 0;
    error_s err_s = call_sasl_func(_sasl_conn.get(), [&]() {
        return sasl_server_start(_sasl_conn.get(),
                                 _selected_mechanism.data(),
                                 input.data(),
                                 input.length(),
                                 &msg,
                                 &msg_len);
    });

    output = blob::create_from_bytes(msg, msg_len);
    return err_s;
}

error_s server_negotiation::do_sasl_step(const blob &input, blob &output)
{
    const char *msg = nullptr;
    unsigned msg_len = 0;
    error_s err_s = call_sasl_func(_sasl_conn.get(), [&]() {
        return sasl_server_step(_sasl_conn.get(), input.data(), input.length(), &msg, &msg_len);
    });

    output = blob::create_from_bytes(msg, msg_len);
    return err_s;
}

error_s server_negotiation::retrive_user_name_from_sasl_conn(std::string &output)
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

void server_negotiation::handle_message_from_client(message_ptr msg)
{
    if (_status == negotiation_status::SASL_LIST_MECHANISMS) {
        on_list_mechanisms(msg);
        return;
    }
    if (_status == negotiation_status::SASL_LIST_MECHANISMS_RESP) {
        on_select_mechanism(msg);
        return;
    }

    handle_client_response_on_challenge(msg);
}

void server_negotiation::handle_client_response_on_challenge(const message_ptr &req)
{
    dinfo("%s: recv response negotiation message from client", _name.c_str());
    negotiation_message client_message;
    dsn::unmarshall(req, client_message);

    if (client_message.status != negotiation_status::SASL_INITIATE &&
        client_message.status != negotiation_status::SASL_RESPONSE) {
        derror("%s: recv wrong negotiation msg, type = %s",
               _name.c_str(),
               enum_to_string(client_message.status));
        fail_negotiation(req, "invalid_client_message_type");
        return;
    }

    dsn::blob output;
    error_s err_s;
    if (client_message.status == negotiation_status::type::SASL_INITIATE) {
        err_s = do_sasl_server_start(client_message.msg, output);
    } else {
        err_s = do_sasl_step(client_message.msg, output);
    }

    if (err_s.code() != ERR_OK && err_s.code() != ERR_INCOMPLETE) {
        dwarn("%s: negotiation failed locally, with err = %s, msg = %s",
              _name.c_str(),
              err_s.code().to_string(),
              err_s.description().c_str());
        fail_negotiation(req, err_s.description());
        return;
    }

    if (err_s.code() == ERR_OK) {
        error_s err = retrive_user_name_from_sasl_conn(_user_name);
        dassert(err.is_ok(), "%s: unexpected result(%s)", _name.c_str(), err.description().c_str());
        ddebug("%s: negotiation succ for user(%s)", _name.c_str(), _user_name.c_str());
        succ_negotiation(req);
    } else {
        negotiation_message challenge;
        challenge.status = negotiation_status::SASL_CHALLENGE;
        challenge.msg = output;
        reply(req, challenge);
    }
}

} // end namespace security
} // end namespace dsn

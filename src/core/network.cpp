/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
# include <dsn/internal/network.h>
# include "rpc_engine.h"

# define __TITLE__ "rpc_session"

namespace dsn {

    rpc_client_session::rpc_client_session(network& net, const end_point& remote_addr, std::shared_ptr<rpc_client_matcher>& matcher)
        : _net(net), _remote_addr(remote_addr), _matcher(matcher)
    {
    }

    void rpc_client_session::call(message_ptr& request, rpc_response_task_ptr& call)
    {
        if (call != nullptr)
        {
            rpc_client_session_ptr sp = this;
            _matcher->on_call(request, call, sp);
        }

        send(request);
    }

    void rpc_client_session::on_disconnected()
    {
        rpc_client_session_ptr sp = this;
        _net.on_client_session_disconnected(sp);
    }

    bool rpc_client_session::on_recv_reply(uint64_t key, message_ptr& reply, int delay_handling_milliseconds)
    {
        if (reply != nullptr)
        {
            reply->header().from_address = remote_address();
            reply->header().to_address = _net.address();
        }

        return _matcher->on_recv_reply(key, reply, delay_handling_milliseconds);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////

    rpc_server_session::rpc_server_session(network& net, const end_point& remote_addr)
        : _remote_addr(remote_addr), _net(net)
    {
    }

    void rpc_server_session::on_recv_request(message_ptr& msg, int delay_handling_milliseconds)
    {
        msg->header().from_address = remote_address();
        msg->header().from_address.port = msg->header().client.port;
        msg->header().to_address = _net.address();

        msg->server_session().reset(this);
        return _net.engine()->on_recv_request(msg, delay_handling_milliseconds);
    }

    void rpc_server_session::on_disconnected()
    {
        rpc_server_session_ptr sp = this;
        return _net.on_server_session_disconnected(sp);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////

    int network::max_faked_port_for_client_only_node = 0;

    network::network(rpc_engine* srv, network* inner_provider)
        : _engine(srv)
    {
    }
    
    std::shared_ptr<rpc_client_matcher> network::new_client_matcher()
    {
        return std::shared_ptr<rpc_client_matcher>(new rpc_client_matcher());
    }

    std::shared_ptr<message_parser> network::new_message_parser()
    {
        // TODO: use factory instead
        message_parser * parser = new dsn_message_parser(1024);
        return std::shared_ptr<message_parser>(parser);
    }

    void network::call(message_ptr& request, rpc_response_task_ptr& call)
    {
        rpc_client_session_ptr client = nullptr;
        end_point& to = request->header().to_address;

        {
            utils::auto_read_lock l(_clients_lock);
            auto it = _clients.find(to);
            if (it != _clients.end())
            {
                client = it->second;
            }
        }

        if (nullptr == client.get())
        {
            utils::auto_write_lock l(_clients_lock);
            auto it = _clients.find(to);
            if (it != _clients.end())
            {
                client = it->second;
            }
            else
            {
                client = create_client_session(to);
                _clients.insert(client_sessions::value_type(to, client));

                // init connection
                client->connect();
            }
        }

        client->call(request, call);
    }

    rpc_server_session_ptr network::get_server_session(const end_point& ep)
    {
        utils::auto_read_lock l(_servers_lock);
        auto it = _servers.find(ep);
        return it != _servers.end() ? it->second : nullptr;
    }

    void network::on_server_session_accepted(rpc_server_session_ptr& s)
    {
        dinfo("server session %s:%u accepted", s->remote_address().name.c_str(), (int)s->remote_address().port);

        utils::auto_write_lock l(_servers_lock);
        _servers.insert(server_sessions::value_type(s->remote_address(), s));

    }

    void network::on_server_session_disconnected(rpc_server_session_ptr& s)
    {
        bool r = false;
        {
            utils::auto_write_lock l(_servers_lock);
            auto it = _servers.find(s->remote_address());
            if (it != _servers.end() && it->second.get() == s.get())
            {
                _servers.erase(it);
                r = true;
            }                
        }

        if (r)
        {
            dinfo("server session %s:%u disconnected", s->remote_address().name.c_str(), (int)s->remote_address().port);
        }
    }

    rpc_client_session_ptr network::get_client_session(const end_point& ep)
    {
        utils::auto_read_lock l(_clients_lock);
        auto it = _clients.find(ep);
        return it != _clients.end() ? it->second : nullptr;
    }

    void network::on_client_session_disconnected(rpc_client_session_ptr& s)
    {
        bool r = false;
        {
            utils::auto_write_lock l(_clients_lock);
            auto it = _clients.find(s->remote_address());
            if (it != _clients.end() && it->second.get() == s.get())
            {
                _clients.erase(it);
                r = true;
            }
        }

        if (r)
        {
            dinfo("client session %s:%u disconnected", s->remote_address().name.c_str(), (int)s->remote_address().port);
        }
    }
}

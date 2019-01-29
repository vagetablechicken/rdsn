#include <dsn/utility/smart_pointers.h>
#include <dsn/tool-api/command_manager.h>
#include <dsn/dist/cli/cli.server.h>

namespace dsn {
class cli_service_impl : public cli_service
{
public:
    void on_call(message_ex *req) override
    {
        if (!_super_user.empty() && req->user_name != _super_user) {
            reply(req, std::string("acl deny"));
            return;
        }
        command request;
        dsn::unmarshall(req, request);

        std::string output;
        dsn::command_manager::instance().run_command(request.cmd, request.arguments, output);
        reply(req, output);
    }
};

std::unique_ptr<cli_service> cli_service::create_service()
{
    return dsn::make_unique<cli_service_impl>();
}
}

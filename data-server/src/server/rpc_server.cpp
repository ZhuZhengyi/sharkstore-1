#include "rpc_server.h"

#include "frame/sf_logger.h"
#include "common/rpc_request.h"
#include "worker.h"
#include "storage/metric.h"
#include "proto/gen/funcpb.pb.h"
#include "proto/gen/kvrpcpb.pb.h"
#include "net/session.h"
#include "net/message.h"

namespace sharkstore {
namespace dataserver {
namespace server {

RPCServer::RPCServer(const net::ServerOptions& ops) :
    ops_(ops) {
}

RPCServer::~RPCServer() {
    Stop();
}

Status RPCServer::Start(const std::string& ip, uint16_t port, Worker* worker) {
    assert(net_server_ == nullptr);
    net_server_.reset(new net::Server(ops_, "rpc"));
    worker_ = worker;
    auto ret = net_server_->ListenAndServe("0.0.0.0", port,
                                           [this](const net::Context& ctx, const net::MessagePtr& msg) {
                                               onMessage(ctx, msg);
                                           });
    if (ret.ok()) {
        FLOG_INFO("RPC Server listen on 0.0.0.0:%u", port);
    }
    return ret;
}

Status RPCServer::Stop() {
    if (net_server_) {
        net_server_->Stop();
        net_server_.reset();
        FLOG_INFO("RPC Server stopped");
    }
    return Status::OK();
}

static void reply(const net::Context& ctx, const net::Head& req_head,
                      const ::google::protobuf::Message& resp) {
    std::vector<uint8_t> body;
    body.resize(resp.ByteSizeLong());
    resp.SerializeToArray(body.data(), static_cast<int>(body.size()));
    ctx.Write(req_head, std::move(body));
}

void RPCServer::onMessage(const net::Context& ctx, const net::MessagePtr& msg) {
    if (msg->head.func_id == funcpb::kFuncInsert) {
        kvrpcpb::DsInsertResponse resp;
        resp.mutable_resp()->set_affected_keys(1);
        reply(ctx, msg->head, resp);
        storage::g_metric.AddWrite(1, 1);
    } else {
        auto task = new RPCRequest(ctx, msg);
        worker_->Push(task);
    }
}

} /* namespace server */
} /* namespace dataserver  */
} /* namespace sharkstore */

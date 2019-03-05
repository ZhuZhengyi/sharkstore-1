#ifndef __CONTEXT_SERVER_H__
#define __CONTEXT_SERVER_H__

#include "common/socket_session.h"
#include "raft/server.h"
#include "storage/db_interface.h"

namespace sharkstore {
namespace dataserver {

namespace storage {
class MetaStore;
}

namespace master {
class Worker;
}

namespace server {

class Worker;
class RunStatus;
class RangeServer;
class RunStatus;

struct ContextServer {
    uint64_t node_id = 0;

    Worker *worker = nullptr;

    RunStatus *run_status = nullptr;
    RangeServer *range_server = nullptr;
    common::SocketSession *socket_session = nullptr;
    master::Worker *master_worker = nullptr;

    storage::DbInterface* db = nullptr;

    storage::MetaStore *meta_store = nullptr;

    raft::RaftServer *raft_server = nullptr;
};

}  // namespace server
}  // namespace dataserver
}  // namespace sharkstore

#endif  // __CONTEXT_SERVER_H__

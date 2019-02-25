#include "worker.h"

#include <assert.h>
#include <chrono>

#include "base/util.h"
#include "common/ds_config.h"
#include "common/ds_proto.h"
#include "frame/sf_config.h"
#include "frame/sf_logger.h"
#include "frame/sf_util.h"
#include "proto/gen/funcpb.pb.h"

#include "callback.h"
#include "run_status.h"
#include "server.h"

namespace sharkstore {
namespace dataserver {
namespace server {

int Worker::Init(ContextServer *context) {
    FLOG_INFO("Worker Init begin ...");

    strcpy(ds_config.worker_config.thread_name_prefix, "work");

    if (socket_server_.Init(&ds_config.worker_config, &worker_status_) != 0) {
        FLOG_ERROR("Worker Init error ...");
        return -1;
    }

    socket_server_.set_recv_done(ds_worker_deal_callback);
    socket_server_.set_send_done(ds_send_done_callback);

    context_ = context;

    FLOG_INFO("Worker Init end ...");
    return 0;
}

void Worker::StartWorker(std::vector<std::thread> &worker,
                         HashQueue &hash_queue, int num) {
    hash_queue.msg_queue.resize(num);

    for (int i = 0; i < num; i++) {
        auto mq = new_lk_queue();
        hash_queue.msg_queue[i] = mq;

        worker.emplace_back([&, mq] {
            common::ProtoMessage *task = nullptr;
            while (g_continue_flag) {
                task = (common::ProtoMessage*)lk_queue_pop(mq); //block mode
                if (task != nullptr) {
                    if (!g_continue_flag) {
                        delete task;
                        break;
                    }
                    --hash_queue.all_msg_size;
                    DealTask(task);
                } else {
                    std::this_thread::sleep_for(std::chrono::microseconds(100));
                }
            }
            FLOG_INFO("Worker thread exit...");
            __sync_fetch_and_sub(&worker_status_.actual_worker_threads, 1);
        });
        __sync_fetch_and_add(&worker_status_.actual_worker_threads, 1);
    }
}

int Worker::Start() {
    FLOG_INFO("Worker Start begin ...");

    // start fast worker
    StartWorker(fast_worker_, fast_queue_, ds_config.fast_worker_num);

    int i = 0;
    char fast_name[32] = {'\0'};
    for (auto &work : fast_worker_) {
        auto handle = work.native_handle();
        snprintf(fast_name, 32, "fast_worker:%d", i++);
        AnnotateThread(handle, fast_name);
    }
    // start slow worker
    StartWorker(slow_worker_, slow_queue_, ds_config.slow_worker_num);

    char slow_name[32] = {'\0'};
    i = 0;
    for (auto &work : slow_worker_) {
        auto handle = work.native_handle();
        snprintf(slow_name, 32, "slow_worker:%d", i++);
        AnnotateThread(handle, slow_name);
    }

    if (socket_server_.Start() != 0) {
        FLOG_ERROR("Worker Start error ...");
        return -1;
    }

    FLOG_INFO("Worker Start end ...");
    return 0;
}

void Worker::Stop() {
    FLOG_INFO("Worker Stop begin ...");

    socket_server_.Stop();

    auto size = fast_worker_.size();
    for (decltype(size) i = 0; i < size; i++) {
        if (fast_worker_[i].joinable()) {
            fast_worker_[i].join();
        }
    }

    size = slow_worker_.size();
    for (decltype(size) i = 0; i < size; i++) {
        if (slow_worker_[i].joinable()) {
            slow_worker_[i].join();
        }
    }

    Clean(fast_queue_);
    Clean(slow_queue_);

    FLOG_INFO("Worker Stop end ...");
}

void Worker::Push(common::ProtoMessage *task) {
    task->socket = &socket_server_;

    auto func_id = static_cast<funcpb::FunctionID>(task->header.func_id);
    if (func_id == 0) { // heart beat
        context_->socket_session->Send(task, nullptr);
    } else if (func_id == funcpb::kFuncInsert) {
        auto resp = new kvrpcpb::DsInsertResponse;
        resp->mutable_resp()->set_affected_keys(1);
        context_->socket_session->Send(task, resp);
    } else {
        FLOG_ERROR("unsupported func id: %s", funcpb::FunctionID_Name(func_id).c_str());
    }

//    if (isSlow(task)) {
//        auto slot = ++slot_seed_ % ds_config.slow_worker_num;
//        auto mq = slow_queue_.msg_queue[slot];
//        lk_queue_push(mq, task);
//        ++slow_queue_.all_msg_size;
//    } else {
//        auto slot = ++slot_seed_ % ds_config.fast_worker_num;
//        auto mq = fast_queue_.msg_queue[slot];
//        lk_queue_push(mq, task);
//        ++fast_queue_.all_msg_size;
//    }
}

void Worker::DealTask(common::ProtoMessage *task) {
    if (task->expire_time < getticks()) {
        FLOG_ERROR("msg_id %" PRIu64 " is expired ", task->header.msg_id);
        delete task;
        return;
    }

    DataServer::Instance().DealTask(task);
}

void Worker::Clean(HashQueue &hash_queue) {
    for (auto mq : hash_queue.msg_queue) {
        while (true) {
            auto task = (common::ProtoMessage *)lk_queue_pop(mq);
            if (task == nullptr) {
                break;
            }
            delete task;
        }
        delete_lk_queue(mq);
    }
}

size_t Worker::ClearQueue(bool fast, bool slow) {
    size_t count = 0;
    if (fast) {
        for (auto& q : fast_queue_.msg_queue) {
            while (true) {
                auto task = (common::ProtoMessage *)lk_queue_pop(q);
                if (task == nullptr) {
                    break;
                }
                delete task;
                ++count;
            }
        }
    }
    if (slow) {
        for (auto& q : slow_queue_.msg_queue) {
            while (true) {
                auto task = (common::ProtoMessage *)lk_queue_pop(q);
                if (task == nullptr) {
                    break;
                }
                delete task;
                ++count;
            }
        }
    }
    return count;
}

bool Worker::isSlow(sharkstore::dataserver::common::ProtoMessage *msg) {
    if ((msg->header.flags & FAST_WORKER_FLAG) != 0) {
        return false;
    }
    switch (msg->header.func_id) {
        case funcpb::FunctionID::kFuncSelect:
        case funcpb::FunctionID::kFuncUpdate:
        case funcpb::FunctionID::kFuncWatchGet:
        case funcpb::FunctionID::kFuncKvRangeDel:
        case funcpb::FunctionID::kFuncKvScan :
            return true;
        default:
            return false;
    }
}

void Worker::PrintQueueSize() {
    FLOG_INFO("worker fast queue size:%" PRIu64,
              fast_queue_.all_msg_size.load());
    FLOG_INFO("worker slow queue size:%" PRIu64,
              slow_queue_.all_msg_size.load());
}

} /* namespace server */
} /* namespace dataserver  */
} /* namespace sharkstore */

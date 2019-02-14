#include "store.h"

#include "base/util.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

using namespace txnpb;
using namespace std::chrono;

static uint64_t calExpireAt(uint64_t ttl) {
    auto seconds = system_clock::now().time_since_epoch();
    return ttl + duration_cast<milliseconds>(seconds).count();
}

static bool isExpired(uint64_t expired_at) {
    auto seconds = system_clock::now().time_since_epoch();
    auto now = duration_cast<milliseconds>(seconds).count();
    return static_cast<uint64_t>(now) > expired_at;
}

static void assignTxnValue(const PrepareRequest& req, const TxnIntent& intent, uint64_t version, TxnValue* value) {
    value->set_txn_id(req.txn_id());
    value->mutable_intent()->CopyFrom(intent);
    value->set_primary_key(req.primary_key());
    value->set_expired_at(calExpireAt(req.lock_ttl()));
    value->set_version(version);
    if (intent.is_primary()) {
        for (const auto& key: req.secondary_keys()) {
            value->add_secondary_keys(key);
        }
    }
}

static void setTxnServerErr(TxnError* err, int32_t code, const std::string& msg) {
    err->set_err_type(TxnError_ErrType_SERVER_ERROR);
    err->mutable_server_err()->set_code(code);
    err->mutable_server_err()->set_msg(msg);
}

static TxnErrorPtr newTxnServerErr(int32_t code, const std::string& msg) {
    TxnErrorPtr err(new TxnError);
    setTxnServerErr(err.get(), code, msg);
    return err;
}

static TxnErrorPtr newLockedError(const TxnValue& value) {
    TxnErrorPtr err(new TxnError);
    err->set_err_type(TxnError_ErrType_LOCKED);

    auto lock_err = err->mutable_lock_err();
    lock_err->set_key(value.intent().key());

    auto lock_info = lock_err->mutable_info();
    lock_info->set_txn_id(value.txn_id());
    lock_info->set_timeout(isExpired(value.expired_at()));
    lock_info->set_is_primary(value.intent().is_primary());
    lock_info->set_primary_key(value.primary_key());
    if (value.intent().is_primary()) {
        lock_info->set_status(value.txn_status());
        for (const auto& skey: value.secondary_keys()) {
            lock_info->add_secondary_keys(skey);
        }
    }
    return err;
}

static TxnErrorPtr newStatusConflictErr(TxnStatus status) {
    TxnErrorPtr err(new TxnError);
    err->set_err_type(TxnError_ErrType_STATUS_CONFLICT);
    err->mutable_status_conflict()->set_status(status);
    return err;
}

// TODO: load from memory
Status Store::getTxnValue(const std::string &key, TxnValue *value) {
    std::string db_value;
    auto s = db_->Get(rocksdb::ReadOptions(), txn_cf_, key, &db_value);
    if (s.IsNotFound()) {
        return Status(Status::kNotFound);
    } else if (!s.ok()) {
        return Status(Status::kIOError, "get txn value", s.ToString());
    }
    if (!value->ParseFromString(db_value)) {
        return Status(Status::kCorruption, "parse txn value", EncodeToHex(db_value));
    }
    assert(value->intent().key() == key);
    return Status::OK();
}

Status Store::writeTxnValue(const txnpb::TxnValue& value, rocksdb::WriteBatch* batch) {
    std::string db_value;
    if (!value.SerializeToString(&db_value)) {
        return Status(Status::kCorruption, "serialze txn value", value.ShortDebugString());
    }
    assert(!value.intent().key().empty());
    auto s = batch->Put(txn_cf_, value.intent().key(), db_value);
    if (!s.ok()) {
        return Status(Status::kIOError, "put txn value", s.ToString());
    } else {
        return Status::OK();
    }
}

TxnErrorPtr Store::checkLockable(const std::string& key, const std::string& txn_id, bool *exist_flag) {
    TxnValue value;
    auto s = getTxnValue(key, &value);
    switch (s.code()) {
    case Status::kNotFound:
        return nullptr;
    case Status::kOk:
        assert(value.intent().key() == key);
        if (value.txn_id() == txn_id) {
            *exist_flag = true;
            return nullptr;
        } else {
            return newLockedError(value);
        }
    default:
        return newTxnServerErr(s.code(), s.ToString());
    }
}

TxnErrorPtr Store::checkUniqueAndVersion(const txnpb::TxnIntent& intent) {
    // TODO:
    // TODO: load version both from txn and data
    return nullptr;
}


TxnErrorPtr Store::prepareIntent(const PrepareRequest& req, const TxnIntent& intent,
        uint64_t version, rocksdb::WriteBatch* batch) {
    // check lockable
    bool exist_flag = false;
    auto err = checkLockable(intent.key(), req.txn_id(), &exist_flag);
    if (err != nullptr) {
        return err;
    }
    if (exist_flag) { // lockable, intent is already written
        return nullptr;
    }

    // check unique and version
    if (intent.check_unique() || intent.expected_ver()) {
        err = checkUniqueAndVersion(intent);
        if (err != nullptr) {
            return err;
        }
    }

    // append to batch
    TxnValue txn_value;
    assignTxnValue(req, intent, version, &txn_value);
    auto s = writeTxnValue(txn_value, batch);
    if (!s.ok()) {
        return newTxnServerErr(s.code(), "serialize txn value failed");
    }
    return nullptr;
}

void Store::TxnPrepare(const PrepareRequest& req, uint64_t version, PrepareResponse* resp) {
    bool primary_lockable = true;
    rocksdb::WriteBatch batch;
    for (const auto& intent: req.intents()) {
        bool stop_flag = false;
        auto err = prepareIntent(req, intent, version, &batch);
        if (err != nullptr) {
            if (err->err_type() == TxnError_ErrType_LOCKED) {
                if (intent.is_primary()) {
                    primary_lockable = false;
                }
            } else { // 其他类型错误，终止prepare
                resp->clear_errors(); // 清除其他错误
                stop_flag = true;
            }
            resp->add_errors()->Swap(err.get());
        }
        if (stop_flag) break;
    }

    if (primary_lockable) {
        auto ret = db_->Write(rocksdb::WriteOptions(), &batch);
        if (!ret.ok()) {
            resp->clear_errors();
            setTxnServerErr(resp->add_errors(), ret.code(), ret.ToString());
        }
    }
}

Status Store::commitIntent(const txnpb::TxnIntent& intent, uint64_t version, rocksdb::WriteBatch* batch) {
    // TODO:
    return Status::OK();
}

TxnErrorPtr Store::decidePrimary(const txnpb::TxnValue& value, txnpb::TxnStatus status, rocksdb::WriteBatch* batch) {
    if (value.txn_status() != INIT) {
        if (value.txn_status() != status) {
            return newStatusConflictErr(value.txn_status());
        } else { // already decided
            return nullptr;
        }
    }

    // txn status is INIT now
    assert(value.txn_status() == INIT);
    // update to new status;
    auto new_value = value;
    new_value.set_txn_status(status);
    auto s = writeTxnValue(value, batch);
    if (!s.ok()) {
        return newTxnServerErr(s.code(), s.ToString());
    }
    // commit intent
    if (status == COMMITTED) {
        s = commitIntent(value.intent(), value.version(), batch);
        if (!s.ok()) {
            return newTxnServerErr(s.code(), s.ToString());
        }
    }
    return nullptr;
}

TxnErrorPtr Store::decideSecondary(const txnpb::TxnValue& value, txnpb::TxnStatus status, rocksdb::WriteBatch* batch) {
    auto ret = batch->Delete(txn_cf_, value.intent().key());
    if (!ret.ok()) {
        return newTxnServerErr(Status::kIOError, ret.ToString());
    }
    if (status == COMMITTED) {
        auto s = commitIntent(value.intent(), value.version(), batch);
        if (!s.ok()) {
            return newTxnServerErr(s.code(), s.ToString());
        }
    }
    return nullptr;
}

TxnErrorPtr Store::decide(const txnpb::DecideRequest& req, const std::string& key, uint64_t& bytes_written,
                   rocksdb::WriteBatch* batch, std::vector<std::string>* secondary_keys) {
    TxnValue value;
    auto s = getTxnValue(key, &value);
    if (!s.ok()) {
        if (s.code() == Status::kNotFound) {
            return nullptr;
        } else {
            return newTxnServerErr(s.code(), s.ToString());
        }
    }

    // s is ok now
    assert(s.ok());
    if (value.txn_id() != req.txn_id()) {
        return nullptr;
    }

    TxnErrorPtr err;
    // decide secondary key
    if (!value.intent().is_primary()) {
        err = decideSecondary(value, req.status(), batch);
    } else {
        err = decidePrimary(value, req.status(), batch);
    }
    if (err != nullptr) {
        return err;
    }

    // add bytes_written
    if (value.intent().typ() == INSERT) {
        bytes_written += value.intent().key().size() + value.intent().value().size();
    }
    // assign secondary_keys in recover mode
    if (secondary_keys != nullptr) {
        for (const auto& skey: value.secondary_keys()) {
            secondary_keys->push_back(skey);
        }
    }
    return nullptr;
}

uint64_t Store::TxnDecide(const DecideRequest& req, DecideResponse* resp) {
    if (req.status() != COMMITTED || req.status() != ABORTED) {
        setTxnServerErr(resp->mutable_err(), Status::kInvalidArgument, "invalid txn status");
        return 0;
    }

    uint64_t bytes_written = 0;
    rocksdb::WriteBatch batch;
    for (const auto& key: req.keys()) {
        TxnErrorPtr err;
        if (req.recover()) { // recover will return secondary keys
            std::vector<std::string> secondary_keys;
            err = decide(req, key, bytes_written, &batch, &secondary_keys);
            if (!secondary_keys.empty()) {
                for (std::size_t i = 0; i < secondary_keys.size(); ++i) {
                    resp->add_secondary_keys(std::move(secondary_keys[i]));
                }
            }
        } else {
            err = decide(req, key, bytes_written, &batch);
        }
        if (err != nullptr) {
            resp->mutable_err()->Swap(err.get());
            return 0;
        }
    }
    auto ret = db_->Write(rocksdb::WriteOptions(), &batch);
    if (!ret.ok()) {
        setTxnServerErr(resp->mutable_err(), Status::kIOError, ret.ToString());
        return 0;
    } else {
        return bytes_written;
    }
}

void Store::TxnClearup(const ClearupRequest& req, ClearupResponse* resp) {
    txnpb::TxnValue value;
    auto s = getTxnValue(req.primary_key(), &value);
    if (!s.ok()) {
        if (s.code() != Status::kNotFound) {
            setTxnServerErr(resp->mutable_err(), s.code(), s.ToString());
        }
        return;
    }
    // s is ok now
    if (value.txn_id() != req.txn_id()) { // success
        return;
    }
    if (!value.intent().is_primary()) {
        setTxnServerErr(resp->mutable_err(), Status::kInvalidArgument, "target key is not primary");
        return;
    }
    // delete intent
    auto ret = db_->Delete(rocksdb::WriteOptions(), txn_cf_, req.primary_key());
    if (!ret.ok()) {
        setTxnServerErr(resp->mutable_err(), Status::kIOError, ret.ToString());
        return;
    }
}

void Store::TxnGetLockInfo(const GetLockInfoRequest& req, GetLockInfoResponse* resp) {
}

void Store::TxnSelect(const SelectRequest& req, SelectResponse* resp) {
}


} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */

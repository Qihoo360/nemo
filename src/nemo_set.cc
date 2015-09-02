#include "nemo.h"
#include "nemo_set.h"
#include "nemo_iterator.h"
#include "utilities/util.h"
#include "xdebug.h"

using namespace nemo;

Status Nemo::SAdd(const std::string &key, const std::string &member, int64_t *res) {
    Status s;
    MutexLock l(&mutex_set_);
    rocksdb::WriteBatch writebatch;
    std::string set_key = EncodeSetKey(key, member);

    std::string val;
    s = set_db_->Get(rocksdb::ReadOptions(), set_key, &val);

    if (s.IsNotFound()) { // not found
        *res = 1;
        if (IncrSSize(key, 1, writebatch) < 0) {
            return Status::Corruption("incrSSize error");
        }
        writebatch.Put(set_key, rocksdb::Slice());
    } else if (s.ok()) {
        *res = 0;
    } else {
        return Status::Corruption("sadd check member error");
    }

    s = set_db_->Write(rocksdb::WriteOptions(), &(writebatch));
    return s;
}

int Nemo::IncrSSize(const std::string &key, int64_t incr, rocksdb::WriteBatch &writebatch) {
    int64_t len = SCard(key);

    if (len == -1) {
        return -1;
    }

    std::string size_key = EncodeSSizeKey(key);

    len += incr;
    if (len == 0) {
        writebatch.Delete(size_key);
    } else {
        writebatch.Put(size_key, rocksdb::Slice((char *)&len, sizeof(int64_t)));
    }
    return 0;
}

int64_t Nemo::SCard(const std::string &key) {
    std::string size_key = EncodeSSizeKey(key);
    std::string val;
    Status s;

    s = set_db_->Get(rocksdb::ReadOptions(), size_key, &val);
    if (s.IsNotFound()) {
        return 0;
    } else if(!s.ok()) {
        return -1;
    } else {
        if (val.size() != sizeof(int64_t)) {
            return 0;
        }
        int64_t ret = *(int64_t *)val.data();
        return ret < 0 ? 0 : ret;
    }
}

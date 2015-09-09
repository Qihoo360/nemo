#include "nemo.h"
#include "nemo_set.h"
#include "nemo_iterator.h"
#include "util.h"
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

Status Nemo::SRem(const std::string &key, const std::string &member, int64_t *res) {
    Status s;
    MutexLock l(&mutex_set_);
    rocksdb::WriteBatch writebatch;
    std::string set_key = EncodeSetKey(key, member);

    std::string val;
    s = set_db_->Get(rocksdb::ReadOptions(), set_key, &val);

    if (s.ok()) {
        *res = 1;
        if (IncrSSize(key, -1, writebatch) < 0) {
            return Status::Corruption("incrSSize error");
        }
        writebatch.Delete(set_key);
        s = set_db_->Write(rocksdb::WriteOptions(), &(writebatch));
    } else if (s.IsNotFound()) {
        *res = 0;
    } else {
        return Status::Corruption("srem check member error");
    }
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

SIterator* Nemo::SScan(const std::string &key, uint64_t limit, bool use_snapshot) {
    std::string set_key = EncodeSetKey(key, "");

    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    if (use_snapshot) {
        iterate_options.snapshot = set_db_->GetSnapshot();
    }
    iterate_options.fill_cache = false;
    it = set_db_->NewIterator(iterate_options);
    it->Seek(set_key);
    return new SIterator(new Iterator(it, "", limit, iterate_options), key); 
}

Status Nemo::SMembers(const std::string &key, std::vector<std::string> &members) {
    SIterator *iter = SScan(key, -1, true);

    while (iter->Next()) {
        members.push_back(iter->Member());
    }
    set_db_->ReleaseSnapshot(iter->Opt().snapshot);
    delete iter;
    return Status::OK();
}

Status Nemo::SUnion(const std::vector<std::string> &keys, std::vector<std::string>& members) {
    std::map<std::string, bool> result_flag;
    
    for (int i = 0; i < (int)keys.size(); i++) {
        SIterator *iter = SScan(keys[i], -1, true);
        
        while (iter->Next()) {
            std::string member = iter->Member();
            if (result_flag.find(member) == result_flag.end()) {
                members.push_back(member);
                result_flag[member] = 1;
            }
        }
        delete iter;
    }
    return Status::OK();
}

Status Nemo::SUnionStore(const std::string &destination, const std::vector<std::string> &keys, int64_t *res) {
    int numkey = keys.size();
    if (numkey <= 0) {
        return Status::Corruption("SInter invalid parameter, no keys");
    } else {   // we don't care that destination is one of the keys.
        Status s;
        int64_t res;
        if (SCard(destination) > 0) {
            SIterator *iter = SScan(destination, -1, true);
            while (iter->Next()) {
                s = SRem(destination, iter->Member(), &res);
                if (!s.ok()) {
                    delete iter;
                    return s;
                }
            }
            delete iter;
        }
    }

    for (int i = 0; i < numkey; i++) {
        SIterator *iter = SScan(keys[i], -1, true);
        
        while (iter->Next()) {
            int64_t add_res;
            Status s = SAdd(destination, iter->Member(), &add_res);
            if (!s.ok()) {
                delete iter;
                return s;
            }
        }
        delete iter;
    }
    *res = SCard(destination);
    return Status::OK();
}

bool Nemo::SIsMember(const std::string &key, const std::string &member) {
    std::string val;

    std::string set_key = EncodeSetKey(key, member);
    Status s = set_db_->Get(rocksdb::ReadOptions(), set_key, &val);

    return s.ok();
}

Status Nemo::SInter(const std::vector<std::string> &keys, std::vector<std::string>& members) {
    int numkey = keys.size();
    if (numkey <= 0) {
        return Status::Corruption("SInter invalid parameter, no keys");
    }

    SIterator *iter = SScan(keys[0], -1, true);
    
    while (iter->Next()) {
        int i = 1;
        std::string member = iter->Member();
        for (; i < numkey; i++) {
            if (!SIsMember(keys[i], member)) {
                break;
            }
        }
        if (i >= numkey) {
            members.push_back(member);
        }
    }
    delete iter;
    return Status::OK();
}

Status Nemo::SInterStore(const std::string &destination, const std::vector<std::string> &keys, int64_t *res) {
    int numkey = keys.size();
    if (numkey <= 0) {
        return Status::Corruption("SInter invalid parameter, no keys");
    } else {   // we don't care that destination is one of the keys.
        Status s;
        int64_t res;
        if (SCard(destination) > 0) {
            SIterator *iter = SScan(destination, -1, true);
            while (iter->Next()) {
                s = SRem(destination, iter->Member(), &res);
                if (!s.ok()) {
                    delete iter;
                    return s;
                }
            }
            delete iter;
        }
    }

    Status s;
    SIterator *iter = SScan(keys[0], -1, true);
    
    while (iter->Next()) {
        int i = 1;
        int64_t add_res;
        std::string member = iter->Member();
        for (; i < numkey; i++) {
            if (!SIsMember(keys[i], member)) {
                break;
            }
        }
        if (i >= numkey) {
            s = SAdd(destination, member, &add_res);
            if (!s.ok()) {
                delete iter;
                return s;
            }
        }
    }
    *res = SCard(destination);
    delete iter;
    return Status::OK();
}

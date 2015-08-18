#include "nemo.h"
#include "nemo_list.h"
#include "utilities/util.h"
#include "xdebug.h"
using namespace nemo;

#define INIT_INDEX (INT_MAX / 2);

static int32_t ParseMeta(std::string &meta, uint64_t &len, uint64_t &left, uint64_t &right) {
    if (meta.size() != sizeof(uint64_t) * 3) {
        return -1;
    }
    len = *((uint64_t *)(meta.data()));
    left = *((uint64_t *)(meta.data() + sizeof(uint64_t)));
    right = *((uint64_t *)(meta.data() + sizeof(uint64_t) * 2));
    return 0;
}

Status Nemo::LIndex(const std::string &key, const int64_t index, std::string *val) {
    Status s;
    rocksdb::WriteBatch batch;
    std::string meta;
    uint64_t len;
    uint64_t left;
    uint64_t right;
    std::string meta_key = EncodeLMetaKey(key);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta);
    if (s.ok()) {
        if (ParseMeta(meta, len, left, right) == 0) {
            uint64_t index_abs = index >= 0 ? left + index + 1 : right + index;
            if (index_abs >= right || index_abs < right - len) {
                return Status::Corruption("index out of range");
            }
            std::string db_key = EncodeListKey(key, std::to_string(index_abs));
            s = list_db_->Get(rocksdb::ReadOptions(), db_key, val);
            return s;
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        return s;
    } else {
        return Status::Corruption("get listmeta error");
    }
}

Status Nemo::LLen(const std::string &key, uint64_t *llen) {
    Status s;
    std::string meta_key = EncodeLMetaKey(key);
    std::string meta;
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta);
    if (s.ok()) {
        if(meta.size() != sizeof(uint64_t) * 3) {
            return Status::Corruption("list meta error");
        }
        *llen = *((uint64_t *)(meta.data()));
    } else {
        *llen = 0;
    }
    return s;
}

Status Nemo::LPush(const std::string &key, const std::string &val, uint64_t *llen) {
    Status s;
    rocksdb::WriteBatch batch;
    std::string meta;
    uint64_t len;
    uint64_t left;
    uint64_t right;
    std::string meta_key = EncodeLMetaKey(key);
    MutexLock l(&mutex_list_);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta);
    if (s.ok()) {
        if (ParseMeta(meta, len, left, right) == 0) {
            if (len == INT_MAX - 2) {
                return Status::Corruption("list reach max length");
            }
            if (left == 0) {
                return Status::Corruption("list left out of range");
            }
            std::string db_key = EncodeListKey(key, std::to_string(left));
            batch.Put(db_key, val);
            len++;
            left--;
            *((uint64_t *)meta.data()) = len;
            *((uint64_t *)(meta.data() + sizeof(uint64_t))) = left;
            batch.Put(meta_key, meta);
            s = list_db_->Write(rocksdb::WriteOptions(), &batch);
            *llen = len;
            return s;
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        len = 1;
        left = INIT_INDEX;
        right = left + 1;
        uint64_t meta[3];
        meta[0] = len;
        meta[1] = left - 1;
        meta[2] = right;
        std::string meta_str((char *)meta, 3 * sizeof(uint64_t));
        batch.Put(meta_key, meta_str);
        batch.Put(EncodeListKey(key, std::to_string(left)), val);
        s = list_db_->Write(rocksdb::WriteOptions(), &batch);
        *llen = len;
        return s;
    } else {
        return Status::Corruption("get listmeta error");
    }
}

Status Nemo::LPop(const std::string &key, std::string *val) {
    Status s;
    rocksdb::WriteBatch batch;
    std::string meta;
    uint64_t len;
    uint64_t left;
    uint64_t right;
    std::string meta_key = EncodeLMetaKey(key);
    MutexLock l(&mutex_list_);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta);
    if (s.ok()) {
        if (ParseMeta(meta, len, left, right) == 0) {
            if (len == 0 || left + 1 == INT_MAX) {
                s = list_db_->Delete(rocksdb::WriteOptions(), meta_key);
                return Status::NotFound("not found the key");
            }
            left++;
            if (--len == 0) {
                batch.Delete(meta_key);
            } else {
                *((uint64_t *)meta.data()) = len;
                *((uint64_t *)(meta.data() + sizeof(uint64_t))) = left;
                batch.Put(meta_key, meta);
            }
            std::string db_key = EncodeListKey(key, std::to_string(left));
            s = list_db_->Get(rocksdb::ReadOptions(), db_key, val);
            batch.Delete(db_key);
            s = list_db_->Write(rocksdb::WriteOptions(), &batch);
            return s;
        }
    } else if (s.IsNotFound()) {
        return Status::NotFound("not found key");
    } else {
        return Status::Corruption("get listmeta error");
    }
}

Status Nemo::LPushx(const std::string &key, const std::string &val, uint64_t *llen) {
    Status s;
    uint64_t len;
    uint64_t left;
    uint64_t right;
    std::string meta;
    std::string meta_key = EncodeLMetaKey(key);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta);
    if (s.ok()) {
        if (ParseMeta(meta, len, left, right) == 0) {
            if (len == 0) {
                *llen = 0;
                return Status::NotFound("not found the key");
            } else if (len > 0) {
                s = LPush(key, val, llen);
                return s;
            } else {
                return Status::Corruption("get invalid listlen");
            }
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        *llen = 0;
        return Status::NotFound("not found the key");
    } else {
        return Status::Corruption("get listmeta error");
    }
}

Status Nemo::LRange(const std::string &key, const int32_t begin, const int32_t end, std::vector<IV> &ivs) {
    Status s;
    uint64_t len;
    uint64_t left;
    uint64_t right;
    std::string meta;
    std::string res;
    std::string meta_key = EncodeLMetaKey(key);
    std::string db_key;
    MutexLock l(&mutex_list_);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta);
    if (s.ok()) {
        if (ParseMeta(meta, len, left, right) == 0) {
            if (len == 0) {
                return Status::NotFound("not found the key");
            } else if (len > 0) {
                uint64_t index_b = begin >= 0 ? left + begin + 1 : right + begin;
                uint64_t index_e = end >= 0 ? left + end + 1 : right + end;
                if (index_b > index_e || index_b > left + len || index_e < right - len) {
                    return Status::OK();
                }
                if (index_b <= left) {
                    index_b = left + 1;
                }
                if (index_e >= right) {
                    index_e = right - 1;
                }
                for (uint64_t i = index_b; i <= index_e; i++) {
                    res = "";
                    db_key = EncodeListKey(key, std::to_string(i));
                    s = list_db_->Get(rocksdb::ReadOptions(), db_key, &res);
                    ivs.push_back(IV{i-left-1, res});
                }
                return Status::OK();
            } else {
                return Status::Corruption("get invalid listlen");
            }
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        return Status::NotFound("not found the key");
    } else {
        return Status::Corruption("get listmeta error");
    }
}

Status Nemo::LSet(const std::string &key, const int32_t index, const std::string &val) {
    Status s;
    uint64_t len;
    uint64_t left;
    uint64_t right;
    std::string meta;
    std::string meta_key = EncodeLMetaKey(key);
    std::string db_key;
    MutexLock l(&mutex_list_);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta);
    if (s.ok()) {
        if (ParseMeta(meta, len, left, right) == 0) {
            if (len == 0) {
                return Status::NotFound("not found the key");
            } else if (len > 0) {
                uint64_t index_pos = index >= 0 ? left + index + 1 : right + index;
                if (index_pos <= left || index_pos >= right) {
                    return Status::Corruption("index out of range");
                }
                db_key = EncodeListKey(key, std::to_string(index_pos));
                s = list_db_->Put(rocksdb::WriteOptions(), db_key, val);
                return s;
            } else {
                return Status::Corruption("get invalid listlen");
            }
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        return Status::NotFound("not found the key");
    } else {
        return Status::Corruption("get listmeta error");
    }
}

Status Nemo::LTrim(const std::string &key, const int32_t begin, const int32_t end) {
    Status s;
    uint64_t len;
    uint64_t left;
    uint64_t right;
    std::string meta;
    std::string meta_key = EncodeLMetaKey(key);
    std::string db_key;
    rocksdb::WriteBatch batch;
    MutexLock l(&mutex_list_);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta);
    if (s.ok()) {
        if (ParseMeta(meta, len, left, right) == 0) {
            if (len == 0) {
                return Status::NotFound("not found the key");
            } else if (len > 0) {
                uint64_t index_b = begin >= 0 ? left + begin + 1 : right + begin;
                uint64_t index_e = end >= 0 ? left + end + 1 : right + end;
                if (index_b > index_e || index_b > left + len || index_e < right - len) {
                    index_b = left;
                    index_e = left;
                }
                if (index_b <= left) {
                    index_b = left + 1;
                }
                if (index_e >= right) {
                    index_e = right - 1;
                }
                uint64_t trim_num = 0;
                for (uint64_t i = left + 1; i < index_b; i++) {
                    db_key = EncodeListKey(key, std::to_string(i));
                    batch.Delete(db_key);
                    trim_num++;
                }
                for (uint64_t i = right - 1; i > index_e; i--) {
                    db_key = EncodeListKey(key, std::to_string(i));
                    batch.Delete(db_key);
                    trim_num++;
                }
                if (trim_num < len) {
                    *((uint64_t *)meta.data()) = len - trim_num;
                    *((uint64_t *)(meta.data() + sizeof(uint64_t))) = index_b - 1;
                    *((uint64_t *)(meta.data() + sizeof(uint64_t) * 2)) = index_e + 1;
                    batch.Put(meta_key, meta);
                } else {
                    batch.Delete(meta_key);
                }
                s = list_db_->Write(rocksdb::WriteOptions(), &batch);
                return s;
            } else {
                return Status::Corruption("get invalid listlen");
            }
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        return Status::NotFound("not found the key");
    } else {
        return Status::Corruption("get listmeta error");
    }
}

Status Nemo::RPush(const std::string &key, const std::string &val, uint64_t *llen) {
    Status s;
    rocksdb::WriteBatch batch;
    std::string meta;
    uint64_t len;
    uint64_t left;
    uint64_t right;
    std::string meta_key = EncodeLMetaKey(key);
    MutexLock l(&mutex_list_);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta);
    if (s.ok()) {
        if (ParseMeta(meta, len, left, right) == 0) {
            if (len == INT_MAX - 2) {
                return Status::Corruption("list reach max length");
            }
            if (right == INT_MAX) {
                return Status::Corruption("list right out of range");
            }
            std::string db_key = EncodeListKey(key, std::to_string(right));
            batch.Put(db_key, val);
            len++;
            right++;
            *((uint64_t *)meta.data()) = len;
            *((uint64_t *)(meta.data() + sizeof(uint64_t) * 2)) = right;
            batch.Put(meta_key, meta);
            s = list_db_->Write(rocksdb::WriteOptions(), &batch);
            *llen = len;
            return s;
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        len = 1;
        left = INIT_INDEX;
        right = left + 1;
        uint64_t meta[3];
        meta[0] = len;
        meta[1] = left;
        meta[2] = right + 1;
        std::string meta_str((char *)meta, 3 * sizeof(uint64_t));
        batch.Put(meta_key, meta_str);
        batch.Put(EncodeListKey(key, std::to_string(right)), val);
        s = list_db_->Write(rocksdb::WriteOptions(), &batch);
        *llen = len;
        return s;
    } else {
        return Status::Corruption("get listmeta error");
    }
}

Status Nemo::RPop(const std::string &key, std::string *val) {
    Status s;
    rocksdb::WriteBatch batch;
    std::string meta;
    uint64_t len;
    uint64_t left;
    uint64_t right;
    std::string meta_key = EncodeLMetaKey(key);
    MutexLock l(&mutex_list_);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta);
    if (s.ok()) {
        if (ParseMeta(meta, len, left, right) == 0) {
            if (len == 0 || right - 1 == 0) {
                s = list_db_->Delete(rocksdb::WriteOptions(), meta_key);
                return Status::NotFound("not found the key");
            }
            right--;
            if (--len == 0) {
                batch.Delete(meta_key);
            } else {
                *((uint64_t *)meta.data()) = len;
                *((uint64_t *)(meta.data() + sizeof(uint64_t) * 2)) = right;
                batch.Put(meta_key, meta);
            }
            std::string db_key = EncodeListKey(key, std::to_string(right));
            s = list_db_->Get(rocksdb::ReadOptions(), db_key, val);
            batch.Delete(db_key);
            s = list_db_->Write(rocksdb::WriteOptions(), &batch);
            return s;
        }
    } else if (s.IsNotFound()) {
        return Status::NotFound("not found key");
    } else {
        return Status::Corruption("get listmeta error");
    }
}

Status Nemo::RPushx(const std::string &key, const std::string &val, uint64_t *llen) {
    Status s;
    uint64_t len;
    uint64_t left;
    uint64_t right;
    std::string meta;
    std::string meta_key = EncodeLMetaKey(key);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta);
    if (s.ok()) {
        if (ParseMeta(meta, len, left, right) == 0) {
            if (len == 0) {
                *llen = 0;
                return Status::NotFound("not found the key");
            } else if (len > 0) {
                s = RPush(key, val, llen);
                return s;
            } else {
                return Status::Corruption("get invalid listlen");
            }
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        *llen = 0;
        return Status::NotFound("not found the key");
    } else {
        return Status::Corruption("get listmeta error");
    }
}

Status Nemo::RPopLPush(const std::string &src, const std::string &dest, std::string &val) {
    Status s;
    rocksdb::WriteBatch batch;
    uint64_t len;
    uint64_t left;
    uint64_t right;
    std::string meta_r;
    std::string meta_l;
    std::string db_key_r;
    std::string meta_key_r = EncodeLMetaKey(src);
    std::string db_key_l;
    std::string meta_key_l = EncodeLMetaKey(dest);
    MutexLock l(&mutex_list_);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key_r, &meta_r);
    if (s.ok()) {
        if (ParseMeta(meta_r, len, left, right) == 0) {
            if (len == 0 || right - 1 == 0) {
                s = list_db_->Delete(rocksdb::WriteOptions(), meta_key_r);
                return Status::NotFound("not found the source key");
            }
            right--;
            if (--len == 0) {
                batch.Delete(meta_key_r);
            } else {
                *((uint64_t *)meta_r.data()) = len;
                *((uint64_t *)(meta_r.data() + sizeof(uint64_t) * 2)) = right;
                batch.Put(meta_key_r, meta_r);
            }
            db_key_r = EncodeListKey(src, std::to_string(right));
            s = list_db_->Get(rocksdb::ReadOptions(), db_key_r, &val);
            batch.Delete(db_key_r);
            s = list_db_->Write(rocksdb::WriteOptions(), &batch);
        }
    } else if (s.IsNotFound()) {
        return Status::NotFound("not found the source key");
    } else {
        return Status::Corruption("get listmeta error");
    }

    s = list_db_->Get(rocksdb::ReadOptions(), meta_key_l, &meta_l);
    if (s.ok()) {
        if (ParseMeta(meta_l, len, left, right) == 0) {
            if (len == INT_MAX - 2) {
                return Status::Corruption("list reach max length");
            }
            if (left == 0) {
                return Status::Corruption("list left out of range");
            }
            db_key_l = EncodeListKey(dest, std::to_string(left));
            batch.Put(db_key_l, val);
            len++;
            left--;
            *((uint64_t *)meta_l.data()) = len;
            *((uint64_t *)(meta_l.data() + sizeof(uint64_t))) = left;
            batch.Put(meta_key_l, meta_l);
            s = list_db_->Write(rocksdb::WriteOptions(), &batch);
            return s;
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        len = 1;
        left = INIT_INDEX;
        right = left + 1;
        uint64_t meta[3];
        meta[0] = len;
        meta[1] = left - 1;
        meta[2] = right;
        std::string meta_str((char *)meta, 3 * sizeof(uint64_t));
        batch.Put(meta_key_l, meta_str);
        batch.Put(EncodeListKey(dest, std::to_string(left)), val);
        s = list_db_->Write(rocksdb::WriteOptions(), &batch);
        return s;
    } else {
        return Status::Corruption("get listmeta error");
    }
}

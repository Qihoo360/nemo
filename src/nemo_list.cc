#include "nemo.h"
#include "nemo_list.h"
#include "util.h"
#include "xdebug.h"
using namespace nemo;

#define INIT_INDEX (INT_MAX / 2);

static int32_t ParseMeta(std::string &meta, int64_t &len, int64_t &left, int64_t &right, int64_t &cur_seq) {
    if (meta.size() != sizeof(int64_t) * 4) {
        return -1;
    }
    len = *((int64_t *)(meta.data()));
    left = *((int64_t *)(meta.data() + sizeof(int64_t)));
    right = *((int64_t *)(meta.data() + sizeof(int64_t) * 2));
    cur_seq = *((int64_t *)(meta.data() + sizeof(int64_t) * 3));
    return 0;
}

int32_t Nemo::L2R(const std::string &key, const int64_t index, const int64_t left, int64_t *priv, int64_t *cur, int64_t *next) {
    int64_t t = index;
    int64_t t_cur = left;
    int64_t seq;
    Status s;
    std::string db_key;
    std::string en_val;
    std::string raw_val;
    while (t >=0 ) {
        *cur = t_cur;
        db_key = EncodeListKey(key, *cur);
        s = list_db_->Get(rocksdb::ReadOptions(), db_key, &en_val); 
        if (!s.ok()) {
            break;
        }
        DecodeListVal(en_val, priv, next, raw_val); 
        t--;
        t_cur = *next;
    }
    if ( t < 0 ) {
        return 0;
    } else {
        return -1;
    }
} 

int32_t Nemo::R2L(const std::string &key, const int64_t index, const int64_t right, int64_t *priv, int64_t *cur, int64_t *next) {
    int64_t t = index;
    int64_t t_cur = right;
    int64_t seq;
    Status s;
    std::string db_key;
    std::string en_val;
    std::string raw_val;
    while (t >= 0) {
        *cur = t_cur;
        db_key = EncodeListKey(key, *cur);
        s = list_db_->Get(rocksdb::ReadOptions(), db_key, &en_val); 
        if (!s.ok()) {
            break;
        }
        DecodeListVal(en_val, priv, next, raw_val); 
        t--;
        t_cur = *priv;
    }
    if ( t < 0 ) {
        return 0;
    } else {
        return -1;
    }
} 

Status Nemo::LIndex(const std::string &key, const int64_t index, std::string *val) {
    Status s;
    rocksdb::WriteBatch batch;
    std::string meta;
    int64_t len;
    int64_t left;
    int64_t right;
    int64_t cur_seq;

    int64_t priv;
    int64_t cur;
    int64_t next;
    std::string en_val;
    std::string meta_key = EncodeLMetaKey(key);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta);
    if (s.ok()) {
        if (ParseMeta(meta, len, left, right, cur_seq) == 0) {
            if ( index >= len || -index > len ) {
                return Status::Corruption("index out of range");
            }
            if (index >= 0) {
                if (L2R(key, index, left, &priv, &cur, &next) != 0) {
                    return Status::Corruption("error in iterate");
                }
            } else {
                if (R2L(key, -index-1, right, &priv, &cur, &next) != 0) {
                    return Status::Corruption("error in iterate");
                }
            }
            std::string db_key = EncodeListKey(key, cur);
            s = list_db_->Get(rocksdb::ReadOptions(), db_key, &en_val);
            DecodeListVal(en_val, &priv, &next, *val);
            return s;
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        return Status::NotFound("not found the key");
    } else {
        return Status::Corruption("get listmeta error");
    }
}

Status Nemo::LLen(const std::string &key, int64_t *llen) {
    Status s;
    std::string meta_key = EncodeLMetaKey(key);
    std::string meta;
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta);
    if (s.ok()) {
        if(meta.size() != sizeof(int64_t) * 4) {
            return Status::Corruption("list meta error");
        }
        *llen = *((int64_t *)(meta.data()));
    } else {
        *llen = 0;
    }
    return s;
}

Status Nemo::LPush(const std::string &key, const std::string &val, int64_t *llen) {
    Status s;
    rocksdb::WriteBatch batch;
    std::string meta;
    int64_t len;
    int64_t left;
    int64_t right;
    int64_t cur_seq;

    int64_t priv;
    int64_t next;
    std::string en_val;
    std::string raw_val;
    std::string meta_key = EncodeLMetaKey(key);
    MutexLock l(&mutex_list_);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta);
    if (s.ok()) {
        if (ParseMeta(meta, len, left, right, cur_seq) == 0) {
            std::string l_key = EncodeListKey(key, left);
            s = list_db_->Get(rocksdb::ReadOptions(), l_key, &en_val);
            if (!s.ok()) {
                return Status::Corruption("get left error");
            }
            DecodeListVal(en_val, &priv, &next, raw_val);
            EncodeListVal(raw_val, cur_seq, next, en_val);
            batch.Put(l_key, en_val);

            std::string db_key = EncodeListKey(key, cur_seq);
            EncodeListVal(val, 0, left, en_val);
            batch.Put(db_key, en_val);

            len++;
            left = cur_seq;
            *((int64_t *)meta.data()) = len;
            *((int64_t *)(meta.data() + sizeof(int64_t))) = left;
            *((int64_t *)(meta.data() + 3 * sizeof(int64_t))) = ++cur_seq;
            batch.Put(meta_key, meta);
            s = list_db_->Write(rocksdb::WriteOptions(), &batch);
            *llen = len;
            return s;
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        int64_t meta[4];
        meta[0] = 1;    //len
        meta[1] = 1;    //left
        meta[2] = 1;    //right
        meta[3] = 2;    //cur_seq
        
        std::string meta_str((char *)meta, 4 * sizeof(int64_t));
        batch.Put(meta_key, meta_str);
        EncodeListVal(val, 0, 0, en_val);
        batch.Put(EncodeListKey(key, 1), en_val);
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
    int64_t len;
    int64_t left;
    int64_t right;
    int64_t cur_seq;

    int64_t priv = 0;
    int64_t next = 0;
    std::string en_val;
    std::string raw_val;
    std::string meta_key = EncodeLMetaKey(key);
    MutexLock l(&mutex_list_);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta);
    if (s.ok()) {
        if (ParseMeta(meta, len, left, right, cur_seq) == 0) {
            std::string db_key = EncodeListKey(key, left);
            s = list_db_->Get(rocksdb::ReadOptions(), db_key, &en_val);
            if (!s.ok()) {
                return Status::Corruption("get left error");
            }
            DecodeListVal(en_val, &priv, &next, *val);

            left = next;

            if (next != 0) {
                std::string l_key = EncodeListKey(key, next);
                s = list_db_->Get(rocksdb::ReadOptions(), l_key, &en_val);
                if (!s.ok()) {
                    return Status::Corruption("get next left error");
                }
                DecodeListVal(en_val, &priv, &next, raw_val);
                EncodeListVal(raw_val, 0, next, en_val);
                batch.Put(l_key, en_val);
            }
            if (--len == 0) {
                batch.Delete(meta_key);
            } else {
                *((int64_t *)meta.data()) = len;
                *((int64_t *)(meta.data() + sizeof(int64_t))) = left;
                batch.Put(meta_key, meta);
            }
            batch.Delete(db_key);
            s = list_db_->Write(rocksdb::WriteOptions(), &batch);
            return s;
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        return Status::NotFound("not found key");
    } else {
        return Status::Corruption("get listmeta error");
    }
}

Status Nemo::LPushx(const std::string &key, const std::string &val, int64_t *llen) {
    Status s;
    int64_t len;
    int64_t left;
    int64_t right;
    int64_t cur_seq;
    std::string meta;
    std::string meta_key = EncodeLMetaKey(key);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta);
    if (s.ok()) {
        if (ParseMeta(meta, len, left, right, cur_seq) == 0) {
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

Status Nemo::LRange(const std::string &key, const int64_t begin, const int64_t end, std::vector<IV> &ivs) {
    Status s;
    int64_t len;
    int64_t left;
    int64_t right;
    int64_t cur_seq;
    std::string meta;
    std::string res;
    std::string meta_key = EncodeLMetaKey(key);
    std::string db_key;
//    MutexLock l(&mutex_list_);
    const rocksdb::Snapshot* ss = list_db_->GetSnapshot();
    rocksdb::ReadOptions rop = rocksdb::ReadOptions();
    rop.snapshot = ss;
    s = list_db_->Get(rop, meta_key, &meta);
    if (s.ok()) {
        if (ParseMeta(meta, len, left, right, cur_seq) == 0) {
            if (len == 0) {
                return Status::NotFound("not found the key");
            } else if (len > 0) {
                int64_t index_b = begin >= 0 ? begin : len + begin;
                int64_t index_e = end >= 0 ? end : len + end;
                if (index_b > index_e || index_b >= len || index_e < 0) {
                    list_db_->ReleaseSnapshot(ss);
                    return Status::OK();
                }
                if (index_b < 0) {
                    index_b = 0;
                }
                if (index_e >= len) {
                    index_e = len - 1;
                }
                int64_t priv;
                int64_t cur;
                int64_t next;
                if (L2R(key, index_b, left, &priv, &cur, &next) != 0) {
                    list_db_->ReleaseSnapshot(ss);
                    return Status::Corruption("error in iterate");
                }
                int32_t t = index_e - index_b + 1;
                std::string i_key;
                std::string i_en_val;
                std::string i_raw_val;
                int32_t i = 0;
                while (i<t) {
                    i_key = EncodeListKey(key, cur);
                    s = list_db_->Get(rocksdb::ReadOptions(), i_key, &i_en_val);
                    if (!s.ok()) {
                        break;
                    }
                    DecodeListVal(i_en_val, &priv, &next, i_raw_val);
                    ivs.push_back(IV{i+index_b, i_raw_val});
                    cur = next;
                    i++;
                }
                if (i<t) {
                    list_db_->ReleaseSnapshot(ss);
                    return Status::Corruption("get element error");
                }
                list_db_->ReleaseSnapshot(ss);
                return Status::OK();
            } else {
                list_db_->ReleaseSnapshot(ss);
                return Status::Corruption("get invalid listlen");
            }
        } else {
            list_db_->ReleaseSnapshot(ss);
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        list_db_->ReleaseSnapshot(ss);
        return Status::NotFound("not found the key");
    } else {
        list_db_->ReleaseSnapshot(ss);
        return Status::Corruption("get listmeta error");
    }
}

Status Nemo::LSet(const std::string &key, const int64_t index, const std::string &val) {
    Status s;
    rocksdb::WriteBatch batch;
    std::string meta;
    int64_t len;
    int64_t left;
    int64_t right;
    int64_t cur_seq;

    int64_t priv;
    int64_t cur;
    int64_t next;
    std::string en_val;
    std::string meta_key = EncodeLMetaKey(key);
    MutexLock l(&mutex_list_);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta);
    if (s.ok()) {
        if (ParseMeta(meta, len, left, right, cur_seq) == 0) {
            if ( index >= len || -index > len ) {
                return Status::Corruption("index out of range");
            }
            if (index >= 0) {
                if (L2R(key, index, left, &priv, &cur, &next) != 0) {
                    return Status::Corruption("error in iterate");
                }
            } else {
                if (R2L(key, -index-1, right, &priv, &cur, &next) != 0) {
                    return Status::Corruption("error in iterate");
                }
            }
            std::string db_key = EncodeListKey(key, cur);
            s = list_db_->Get(rocksdb::ReadOptions(), db_key, &en_val);
            std::string raw_val;
            DecodeListVal(en_val, &priv, &next, raw_val);
            EncodeListVal(val, priv, next, en_val);
            s = list_db_->Put(rocksdb::WriteOptions(), db_key, en_val);
            return s;
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        return Status::NotFound("not found the key");
    } else {
        return Status::Corruption("get listmeta error");
    }
}

Status Nemo::LTrim(const std::string &key, const int64_t begin, const int64_t end) {
    Status s;
    int64_t len;
    int64_t left;
    int64_t right;
    int64_t cur_seq;
    std::string meta;
    std::string meta_key = EncodeLMetaKey(key);
    std::string db_key;
    rocksdb::WriteBatch batch;
    MutexLock l(&mutex_list_);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta);
    if (s.ok()) {
        if (ParseMeta(meta, len, left, right, cur_seq) == 0) {
            if (len == 0) {
                return Status::NotFound("not found the key");
            } else if (len > 0) {
                int64_t index_b = begin >= 0 ? begin : len + begin;
                int64_t index_e = end >= 0 ? end : len + end;
                if (index_b > index_e || index_b >= len || index_e < 0) {
                    index_b = len;
                    index_e = len;
                }
                if (index_b < 0) {
                    index_b = 0;
                }
                if (index_e >= len) {
                    index_e = len - 1;
                }
                int64_t trim_num = 0;
                int64_t t_cur;
                int64_t priv = 0;
                int64_t next = 0;
                std::string en_val;
                std::string raw_val;
                t_cur = left;
                for (int64_t i = 0; i < index_b; i++) {
                    db_key = EncodeListKey(key, t_cur);
                    s = list_db_->Get(rocksdb::ReadOptions(), db_key, &en_val);
                    DecodeListVal(en_val, &priv, &next, raw_val);
                    batch.Delete(db_key);
                    t_cur = next;
                    trim_num++;
                }
                if (next != 0) {
                    *((int64_t *)(meta.data() + sizeof(int64_t))) = next;
                    std::string l_key = EncodeListKey(key, next);
                    s = list_db_->Get(rocksdb::ReadOptions(), l_key, &en_val);
                    if (!s.ok()) {
                        return Status::Corruption("get next left error");
                    }
                    DecodeListVal(en_val, &priv, &next, raw_val);
                    EncodeListVal(raw_val, 0, next, en_val);
                    s = list_db_->Put(rocksdb::WriteOptions(), l_key, en_val);
                }
                t_cur = right;
                for (int64_t i = len - 1; i > index_e; i--) {
                    db_key = EncodeListKey(key, t_cur);
                    s = list_db_->Get(rocksdb::ReadOptions(), db_key, &en_val);
                    DecodeListVal(en_val, &priv, &next, raw_val);
                    batch.Delete(db_key);
                    t_cur = priv;
                    trim_num++;
                }
                if (priv != 0) {
                    *((int64_t *)(meta.data() + sizeof(int64_t) * 2)) = priv;
                    std::string r_key = EncodeListKey(key, priv);
                    s = list_db_->Get(rocksdb::ReadOptions(), r_key, &en_val);
                    if (!s.ok()) {
                        return Status::Corruption("get next right error");
                    }
                    DecodeListVal(en_val, &priv, &next, raw_val);
                    EncodeListVal(raw_val, priv, 0, en_val);
                    s = list_db_->Put(rocksdb::WriteOptions(), r_key, en_val);
                }
                if (trim_num < len) {
                    *((int64_t *)meta.data()) = len - trim_num;
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

Status Nemo::RPush(const std::string &key, const std::string &val, int64_t *llen) {
    Status s;
    rocksdb::WriteBatch batch;
    std::string meta;
    int64_t len;
    int64_t left;
    int64_t right;
    int64_t cur_seq;

    int64_t priv;
    int64_t next;
    std::string en_val;
    std::string raw_val;
    std::string meta_key = EncodeLMetaKey(key);
    MutexLock l(&mutex_list_);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta);
    if (s.ok()) {
        if (ParseMeta(meta, len, left, right, cur_seq) == 0) {
            std::string r_key = EncodeListKey(key, right);
            s = list_db_->Get(rocksdb::ReadOptions(), r_key, &en_val);
            if (!s.ok()) {
                return Status::Corruption("get right error");
            }
            DecodeListVal(en_val, &priv, &next, raw_val);
            EncodeListVal(raw_val, priv, cur_seq, en_val);
            batch.Put(r_key, en_val);

            std::string db_key = EncodeListKey(key, cur_seq);
            EncodeListVal(val, right, 0, en_val);
            batch.Put(db_key, en_val);

            len++;
            right = cur_seq;
            *((int64_t *)meta.data()) = len;
            *((int64_t *)(meta.data() + 2 * sizeof(int64_t))) = right;
            *((int64_t *)(meta.data() + 3 * sizeof(int64_t))) = ++cur_seq;
            batch.Put(meta_key, meta);
            s = list_db_->Write(rocksdb::WriteOptions(), &batch);
            *llen = len;
            return s;
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        int64_t meta[4];
        meta[0] = 1;    //len
        meta[1] = 1;    //left
        meta[2] = 1;    //right
        meta[3] = 2;    //cur_seq
        
        std::string meta_str((char *)meta, 4 * sizeof(int64_t));
        batch.Put(meta_key, meta_str);
        EncodeListVal(val, 0, 0, en_val);
        batch.Put(EncodeListKey(key, 1), en_val);
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
    int64_t len;
    int64_t left;
    int64_t right;
    int64_t cur_seq;

    int64_t priv = 0;
    int64_t next = 0;
    std::string en_val;
    std::string raw_val;
    std::string meta_key = EncodeLMetaKey(key);
    MutexLock l(&mutex_list_);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta);
    if (s.ok()) {
        if (ParseMeta(meta, len, left, right, cur_seq) == 0) {
            std::string db_key = EncodeListKey(key, right);
            s = list_db_->Get(rocksdb::ReadOptions(), db_key, &en_val);
            if (!s.ok()) {
                return Status::Corruption("get right error");
            }
            DecodeListVal(en_val, &priv, &next, *val);

            right = priv;

            if (priv != 0) {
                std::string r_key = EncodeListKey(key, priv);
                s = list_db_->Get(rocksdb::ReadOptions(), r_key, &en_val);
                if (!s.ok()) {
                    return Status::Corruption("get next right error");
                }
                DecodeListVal(en_val, &priv, &next, raw_val);
                EncodeListVal(raw_val, priv, 0, en_val);
                batch.Put(r_key, en_val);
            }
            
            if (--len == 0) {
                batch.Delete(meta_key);
            } else {
                *((int64_t *)meta.data()) = len;
                *((int64_t *)(meta.data() + 2 * sizeof(int64_t))) = right;
                batch.Put(meta_key, meta);
            }
            batch.Delete(db_key);
            s = list_db_->Write(rocksdb::WriteOptions(), &batch);
            return s;
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        return Status::NotFound("not found key");
    } else {
        return Status::Corruption("get listmeta error");
    }
}

Status Nemo::RPushx(const std::string &key, const std::string &val, int64_t *llen) {
    Status s;
    int64_t len;
    int64_t left;
    int64_t right;
    int64_t cur_seq;
    std::string meta;
    std::string meta_key = EncodeLMetaKey(key);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta);
    if (s.ok()) {
        if (ParseMeta(meta, len, left, right, cur_seq) == 0) {
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
    int64_t len;
    int64_t left;
    int64_t right;
    int64_t cur_seq;
    std::string meta_r;
    std::string meta_l;
    std::string db_key_r;
    std::string meta_key_r = EncodeLMetaKey(src);
    std::string db_key_l;
    std::string meta_key_l = EncodeLMetaKey(dest);

    MutexLock l(&mutex_list_);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key_r, &meta_r);
    if (s.ok()) {
        if (ParseMeta(meta_r, len, left, right, cur_seq) == 0) {
            if (len == 0 || right == 0) {
                s = list_db_->Delete(rocksdb::WriteOptions(), meta_key_r);
                return Status::NotFound("not found the source key");
            }
            std::string en_val;
            std::string raw_val;
            int64_t priv = 0;
            int64_t next = 0;
            db_key_r = EncodeListKey(src, right);
            s = list_db_->Get(rocksdb::ReadOptions(), db_key_r, &en_val);
            DecodeListVal(en_val, &priv, &next, val);
            right = priv;
            if (priv != 0) {
                std::string r_key = EncodeListKey(src, priv);
                s = list_db_->Get(rocksdb::ReadOptions(), r_key, &en_val);
                if (!s.ok()) {
                    return Status::Corruption("get next right error");
                }
                DecodeListVal(en_val, &priv, &next, raw_val);
                EncodeListVal(raw_val, priv, 0, en_val);
                batch.Put(r_key, en_val);
            }
            if (--len == 0) {
                batch.Delete(meta_key_r);
            } else {
                *((int64_t *)meta_r.data()) = len;
                *((int64_t *)(meta_r.data() + sizeof(int64_t) * 2)) = right;
                s = list_db_->Put(rocksdb::WriteOptions(), meta_key_r, meta_r);
            }
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
        if (ParseMeta(meta_l, len, left, right, cur_seq) == 0) {
            db_key_l = EncodeListKey(dest, left);
            std::string raw_val;
            std::string en_val;
            int64_t priv = 0;
            int64_t next = 0;
            s = list_db_->Get(rocksdb::ReadOptions(), db_key_l, &en_val);
            DecodeListVal(en_val, &priv, &next, raw_val);
            EncodeListVal(raw_val, cur_seq, next, en_val);
            batch.Put(db_key_l, en_val);

            db_key_l = EncodeListKey(dest, cur_seq);
            EncodeListVal(val, 0, left, en_val);
            batch.Put(db_key_l, en_val);
            len++;
            *((int64_t *)meta_l.data()) = len;
            *((int64_t *)(meta_l.data() + sizeof(int64_t))) = cur_seq;
            *((int64_t *)(meta_l.data() + 3 * sizeof(int64_t))) = ++cur_seq;
            batch.Put(meta_key_l, meta_l);
            s = list_db_->Write(rocksdb::WriteOptions(), &batch);
            return s;
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        int64_t meta[4];
        meta[0] = 1;    //len
        meta[1] = 1;    //left
        meta[2] = 1;    //right
        meta[3] = 2;    //cur_seq
        std::string en_val;
        
        std::string meta_str((char *)meta, 4 * sizeof(int64_t));
        batch.Put(meta_key_l, meta_str);
        EncodeListVal(val, 0, 0, en_val);
        batch.Put(EncodeListKey(dest, 1), en_val);
        s = list_db_->Write(rocksdb::WriteOptions(), &batch);
        return s;
    } else {
        return Status::Corruption("get listmeta error");
    }
}

Status Nemo::LInsert(const std::string &key, Position pos, const std::string &pivot, const std::string &val, int64_t *llen) {
    Status s;
    rocksdb::WriteBatch batch;
    std::string meta;
    int64_t len;
    int64_t left;
    int64_t right;
    int64_t cur_seq;
    int64_t before_seq;
    int64_t after_seq;

    int64_t priv;
    int64_t next;
    std::string en_val;
    std::string raw_val;
    std::string meta_key = EncodeLMetaKey(key);
    MutexLock l(&mutex_list_);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta);
    if (s.ok()) {
        if (ParseMeta(meta, len, left, right, cur_seq) == 0) {

            // traverse to find pivot
            next = left;
            int64_t tmp_seq;
            do {
              tmp_seq = next;
              std::string cur_listkey = EncodeListKey(key, tmp_seq);
              s = list_db_->Get(rocksdb::ReadOptions(), cur_listkey, &en_val);
              if (!s.ok()) {
                return Status::Corruption("get listkey error");
              }
              DecodeListVal(en_val, &priv, &next, raw_val);
            } while (next != 0 && raw_val != pivot);

            if (raw_val == pivot) {
                if (pos == AFTER) {
                    before_seq = tmp_seq;
                    after_seq = next;
                } else {
                    before_seq = priv;
                    after_seq = tmp_seq;
                }

                // modify before node
                if (before_seq != 0) {
                    std::string cur_listkey = EncodeListKey(key, before_seq);
                    s = list_db_->Get(rocksdb::ReadOptions(), cur_listkey, &en_val);
                    if (!s.ok()) {
                        return Status::Corruption("get listkey error");
                    }
                    DecodeListVal(en_val, &priv, &next, raw_val);
                    EncodeListVal(raw_val, priv, cur_seq, en_val);
                    batch.Put(cur_listkey, en_val);
                }
                // modify after node
                if (after_seq != 0) {
                    std::string cur_listkey = EncodeListKey(key, after_seq);
                    s = list_db_->Get(rocksdb::ReadOptions(), cur_listkey, &en_val);
                    if (!s.ok()) {
                        return Status::Corruption("get listkey error");
                    }
                    DecodeListVal(en_val, &priv, &next, raw_val);
                    EncodeListVal(raw_val, cur_seq, next, en_val);
                    batch.Put(cur_listkey, en_val);
                }
                // add new node
                std::string add_key = EncodeListKey(key, cur_seq);
                EncodeListVal(val, before_seq, after_seq, en_val);
                batch.Put(add_key, en_val);

                len++;
                if (before_seq == 0) {
                    left = cur_seq;
                }
                *((int64_t *)meta.data()) = len;
                *((int64_t *)(meta.data() + sizeof(int64_t))) = left;
                *((int64_t *)(meta.data() + 3 * sizeof(int64_t))) = ++cur_seq;
                batch.Put(meta_key, meta);
                s = list_db_->Write(rocksdb::WriteOptions(), &batch);
                *llen = len;
                return s;
            } else {
                *llen = -1;
                return s;
            }
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        *llen = 0;
        return s;
    } else {
        *llen = 0;
        return Status::Corruption("get key error");
    }
}

Status Nemo::LRem(const std::string &key, const int64_t count, const std::string &val, int64_t *rem_count) {
    Status s;
    rocksdb::WriteBatch batch;
    std::string meta;
    int64_t len;
    int64_t left;
    int64_t right;
    int64_t cur_seq;
    int64_t before_seq;
    int64_t after_seq;

    int64_t priv;
    int64_t next;
    std::string en_val;
    std::string raw_val;
    std::string meta_key = EncodeLMetaKey(key);
    MutexLock l(&mutex_list_);
    s = list_db_->Get(rocksdb::ReadOptions(), meta_key, &meta);
    if (s.ok()) {
        if (ParseMeta(meta, len, left, right, cur_seq) == 0) {
            int64_t tmp_seq;
            int64_t tmp_priv, tmp_next;
            priv = right;
            next = left;
            *rem_count = 0;

            int64_t total_rem = abs(count);
            if (total_rem > len) {
                total_rem = len;
            }

            do {
              if (len == 0) return s;

              if (count < 0) {
                  tmp_seq = priv;
              } else {
                  tmp_seq = next;
              }

              std::string cur_listkey = EncodeListKey(key, tmp_seq);
              s = list_db_->Get(rocksdb::ReadOptions(), cur_listkey, &en_val);
              if (!s.ok()) {
                return Status::Corruption("get listkey error");
              }
              DecodeListVal(en_val, &priv, &next, raw_val);

              if (raw_val == val) {
                (*rem_count)++;

                // modify before node
                if (priv != 0) {
                    std::string priv_listkey = EncodeListKey(key, priv);
                    s = list_db_->Get(rocksdb::ReadOptions(), priv_listkey, &en_val);
                    if (!s.ok()) {
                        return Status::Corruption("get listkey error");
                    }
                    DecodeListVal(en_val, &tmp_priv, &tmp_next, raw_val);
                    EncodeListVal(raw_val, tmp_priv, next, en_val);
                    batch.Put(priv_listkey, en_val);
                }
                // modify after node
                if (next != 0) {
                    std::string next_listkey = EncodeListKey(key, next);
                    s = list_db_->Get(rocksdb::ReadOptions(), next_listkey, &en_val);
                    if (!s.ok()) {
                        return Status::Corruption("get listkey error");
                    }
                    DecodeListVal(en_val, &tmp_priv, &tmp_next, raw_val);
                    EncodeListVal(raw_val, priv, tmp_next, en_val);
                    batch.Put(next_listkey, en_val);
                }
                // delete new node
                batch.Delete(cur_listkey);

                len--;
                if (len == 0) {
                  batch.Delete(meta_key);
                } else {
                  if (priv == 0) {
                    left = next;
                  }
                  if (next == 0) {
                    right = priv;
                  }
                  *((int64_t *)meta.data()) = len;
                  *((int64_t *)(meta.data() + sizeof(int64_t))) = left;
                  *((int64_t *)(meta.data() + 2 * sizeof(int64_t))) = right;
                  batch.Put(meta_key, meta);
                }
                s = list_db_->Write(rocksdb::WriteOptions(), &batch);
              }
            } while ((count == 0 || *rem_count < total_rem) 
                     && ((count < 0 && priv != 0) || (count >= 0 && next != 0)));
            return Status::OK();
        } else {
            return Status::Corruption("parse listmeta error");
        }
    } else if (s.IsNotFound()) {
        *rem_count = 0;
        return s;
    } else {
        return Status::Corruption("get key error");
    }
}

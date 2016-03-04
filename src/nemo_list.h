#ifndef NEMO_INCLUDE_NEMO_LIST_H
#define NEMO_INCLUDE_NEMO_LIST_H

#include "nemo.h"
#include "nemo_const.h"
#include "decoder.h"

namespace nemo {

struct ListMeta : public NemoMeta {
  int64_t len;
  int64_t left;
  int64_t right;
  int64_t cur_seq;

  ListMeta() : len(0), left(0), right(0), cur_seq(1) {}
  ListMeta(int64_t _len, int64_t _left, int64_t _right, int64_t cseq)
      : len(_len), left(_left), right(_right), cur_seq(cseq) {}
  virtual bool DecodeFrom(const std::string& raw_meta);
  virtual bool EncodeTo(std::string& raw_meta);
  virtual std::string ToString();
};

inline std::string EncodeLMetaKey(const rocksdb::Slice &key) {
    std::string buf;
    buf.append(1, DataType::kLMeta);
    buf.append(key.data(), key.size());
    return buf;
}

inline int DecodeLMetaKey(const rocksdb::Slice &slice, std::string *key) {
    Decoder decoder(slice.data(), slice.size());
    if (decoder.Skip(1) == -1) {
        return -1;
    }
    if (decoder.ReadData(key) == -1) {
        return -1;
    }
    return 0;
}

inline std::string EncodeListKey(const rocksdb::Slice &key, const int64_t seq) {
    std::string buf;
    buf.append(1, DataType::kList);
    buf.append(1, (uint8_t)key.size());
    buf.append(key.data(), key.size());
    buf.append((char *)&seq, sizeof(int64_t));
    return buf;
}

inline int DecodeListKey(const rocksdb::Slice &slice, std::string *key, int64_t *seq) {
    Decoder decoder(slice.data(), slice.size());
    if (decoder.Skip(1) == -1) {
        return -1;
    }
    if (decoder.ReadLenData(key) == -1) {
        return -1;
    }
    if (decoder.ReadInt64(seq) == -1) {
        return -1;
    }
    return 0;
}

inline void EncodeListVal(const std::string &raw_val, const int64_t priv, const int64_t next, std::string &en_val) {
    en_val.clear();
    en_val.append((char *)&priv, sizeof(int64_t));
    en_val.append((char *)&next, sizeof(int64_t));
    en_val.append(raw_val.data(), raw_val.size());
}

inline void DecodeListVal(const std::string &en_val, int64_t *priv, int64_t *next, std::string &raw_val) {
    raw_val.clear();
    *priv = *((int64_t *)en_val.data());
    *next = *((int64_t *)(en_val.data() + sizeof(int64_t)));
    raw_val = en_val.substr(sizeof(int64_t) * 2, en_val.size() - sizeof(int64_t) * 2);
}

}

#endif

#ifndef NEMO_INCLUDE_NEMO_LIST_H_
#define NEMO_INCLUDE_NEMO_LIST_H_

#include "nemo.h"
#include "nemo_const.h"
#include "utilities/decoder.h"

namespace nemo {

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

inline std::string EncodeListKey(const rocksdb::Slice &key, const uint64_t seq) {
    std::string buf;
    buf.append(1, DataType::kList);
    buf.append(1, (uint8_t)key.size());
    buf.append(key.data(), key.size());
    buf.append((char *)&seq, sizeof(uint64_t));
    return buf;
}

inline int DecodeListKey(const rocksdb::Slice &slice, std::string *key, uint64_t *seq) {
    Decoder decoder(slice.data(), slice.size());
    if (decoder.Skip(1) == -1) {
        return -1;
    }
    if (decoder.ReadLenData(key) == -1) {
        return -1;
    }
    if (decoder.ReadUInt64(seq) == -1) {
        return -1;
    }
    return 0;
}

inline std::string EncodeListVal(const std::string &raw_val, const uint64_t priv, const uint64_t next, std::string &en_val) {
    en_val.append((char *)&priv, sizeof(uint64_t));
    en_val.append((char *)&next, sizeof(uint64_t));
    en_val.append(raw_val.data(), raw_val.size());
}

inline std::string DecodeListVal(const std::string &en_val, uint64_t *priv, uint64_t *next, std::string &raw_val) {
    *priv = *((uint64_t *)en_val.data());
    *next = *((uint64_t *)(en_val.data() + sizeof(uint64_t));
    raw_val = en_val.substr(sizeof(uint64_t) * 2, en_val.size() - sizeof(uint64_t) * 2);
}

}

#endif

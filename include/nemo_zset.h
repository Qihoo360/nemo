#ifndef NEMO_INCLUDE_NEMO_LIST_H_
#define NEMO_INCLUDE_NEMO_LIST_H_

#include "nemo.h"
#include "nemo_const.h"
#include "utilities/decoder.h"
#include "utilities/strings.h"
#include <stdint.h>

namespace nemo {

inline std::string EncodeZSetKey(const rocksdb::Slice &key, const rocksdb::Slice &member) {
    std::string buf;
    buf.append(1, DataType::kZSet);
    buf.append(1, (uint8_t)key.size());
    buf.append(key.data(), key.size());
    buf.append(1, '=');
    buf.append(member.data(), member.size());
    return buf;
}

inline int DecodeZSetKey(const rocksdb::Slice &slice, std::string *key, std::string *member) {
    Decoder decoder(slice.data(), slice.size());
    if (decoder.Skip(1) == -1) {
        return -1;
    }
    if (decoder.ReadLenData(key) == -1) {
        return -1;
    }
    if (decoder.Skip(1) == -1) {
        return -1;
    }
    if (decoder.ReadData(member) == -1) {
        return -1;
    }
    return 0;
}

inline std::string EncodeZSizeKey(const rocksdb::Slice key) {
    std::string buf;
    buf.append(1, DataType::kZSize);
    buf.append(key.data(), key.size());
    return buf;
}

inline int DecodeZSizeKey(const rocksdb::Slice &slice, std::string *size) {
    Decoder decoder(slice.data(), slice.size());
    if (decoder.Skip(1) == -1) {
        return -1;
    }
    if (decoder.ReadData(size) == -1) {
        return -1;
    }
    return 0;
}

inline std::string EncodeZScoreKey(const rocksdb::Slice &key, const rocksdb::Slice &member, int64_t score) {
    std::string buf;
    buf.append(1, DataType::kZScore);
    uint8_t t = (uint8_t)key.size();
    buf.append(1, (uint8_t)key.size());
    buf.append(key.data(), key.size());
    if (score >= 0) {
        buf.append(1, '=');
    } else {
        buf.append(1, '-');
    }
    score = htobe64(score);
    buf.append((char *)&score, sizeof(int64_t));
    buf.append(1, '=');
    buf.append(member.data(), member.size());
    return buf;
}

inline int DecodeZScoreKey(const rocksdb::Slice &slice, std::string *key, std::string *member, int64_t *score) {
    Decoder decoder(slice.data(), slice.size());
    if (decoder.Skip(1) == -1) {
        return -1;
    }
    if (decoder.ReadLenData(key) == -1) {
        return -1;
    }
    if (decoder.Skip(1) == -1) {
        return -1;
    }
    decoder.ReadInt64(score);
    if (decoder.Skip(1) == -1) {
        return -1;
    }
    if (decoder.ReadData(member) == -1) {
        return -1;
    }
    return 0;    
    
}
}
#endif


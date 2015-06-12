#ifndef NEMO_INCLUDE_NEMO_LIST_H_
#define NEMO_INCLUDE_NEMO_LIST_H_

#include "nemo.h"
#include "nemo_const.h"
#include "utilities/decoder.h"

namespace nemo
{

inline std::string EncodeLMetaKey(const rocksdb::Slice &name) {
    std::string buf;
    buf.append(1, DataType::kLMeta);
    buf.append(name.data(), name.size());
    return buf;
}

inline int DecodeLMetaKey(const rocksdb::Slice &slice, std::string *name) {
    Decoder decoder(slice.data(), slice.size());
    if (decoder.Skip(1) == -1) {
        return -1;
    }
    if (decoder.ReadData(name) == -1) {
        return -1;
    }
    return 0;
}

inline std::string EncodeListKey(const rocksdb::Slice &name, const rocksdb::Slice &key) {
    std::string buf;
    buf.append(1, DataType::kList);
    buf.append(1, (uint8_t)name.size());
    buf.append(name.data(), name.size());
    buf.append(1, '=');
    buf.append(key.data(), key.size());
    return buf;
}

inline int DecodeListKey(const rocksdb::Slice &slice, std::string *name, std::string *key) {
    Decoder decoder(slice.data(), slice.size());
    if (decoder.Skip(1) == -1) {
        return -1;
    }
    if (decoder.ReadLenData(name) == -1) {
        return -1;
    }
    if (decoder.Skip(2) == -1) {
        return -1;
    }
    if (decoder.ReadData(key) == -1) {
        return -1;
    }
    return 0;
}

}

#endif

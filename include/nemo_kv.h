#ifndef NEMO_INCLUDE_NEMO_KV_H
#define NEMO_INCLUDE_NEMO_KV_H

#include "nemo.h"
#include "nemo_const.h"
#include "utilities/decoder.h"

namespace nemo {

inline std::string EncodeKvKey(const rocksdb::Slice &key) {
    std::string buf;
    buf.append(1, DataType::kKv);
    buf.append(key.data(), key.size());
    return buf;
}

inline int DecodeKvKey(const rocksdb::Slice &slice, std::string *key) {
    Decoder decoder(slice.data(), slice.size());
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

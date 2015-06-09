#ifndef NEMO_INCLUDE_NEMO_KV_H
#define NEMO_INCLUDE_NEMO_KV_H

#include "nemo.h"
#include "nemo_const.h"
#include "utilities/decoder.h"

namespace nemo {

static inline
std::string encode_kv_key(const rocksdb::Slice &key){
    std::string buf;
    buf.append(1, DataType::kKv);
    buf.append(key.data(), key.size());
    return buf;
}

static inline
int decode_kv_key(const rocksdb::Slice &slice, std::string *key){
    Decoder decoder(slice.data(), slice.size());
    if(decoder.skip(1) == -1){
        return -1;
    }
    if(decoder.read_data(key) == -1){
        return -1;
    }
    return 0;
}
}


#endif

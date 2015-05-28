#ifndef NEMO_INCLUDE_NEMO_KV_H
#define NEMO_INCLUDE_NEMO_KV_H

#include "nemo.h"
#include "nemo_const.h"
//#include "nemo_iterator.h"
#include "utilities/decoder.h"
//namespace rocksdb {
//   class Slice;
//}

namespace nemo {

inline static
std::string encode_hsize_key(const rocksdb::Slice &name){
    std::string buf;
    buf.append(1, DataType::HSIZE);
    buf.append(name.data(), name.size());
    return buf;
}

inline static
int decode_hsize_key(const rocksdb::Slice &slice, std::string *name){
    Decoder decoder(slice.data(), slice.size());
    if(decoder.skip(1) == -1){
        return -1;
    }
    if(decoder.read_data(name) == -1){
        return -1;
    }
    return 0;
}

inline static
std::string encode_hash_key(const rocksdb::Slice &name, const rocksdb::Slice &key){
    std::string buf;
    buf.append(1, DataType::HASH);
    buf.append(1, (uint8_t)name.size());
    buf.append(name.data(), name.size());
    buf.append(1, '=');
    buf.append(key.data(), key.size());
    return buf;
}

inline static
int decode_hash_key(const rocksdb::Slice &slice, std::string *name, std::string *key){
    Decoder decoder(slice.data(), slice.size());
    if(decoder.skip(1) == -1){
        return -1;
    }
    if(decoder.read_8_data(name) == -1){
        return -1;
    }
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

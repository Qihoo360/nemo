#ifndef NEMO_INCLUDE_NEMO_CONST_H_
#define NEMO_INCLUDE_NEMO_CONST_H_

namespace nemo {

struct KV {
    std::string key;
    std::string val;
};
struct KVS {
    std::string key;
    std::string val;
    rocksdb::Status status;
};

struct FV {
    std::string field;
    std::string val;
};
struct FVS {
    std::string field;
    std::string val;
    rocksdb::Status status;
};

struct IV {
    uint64_t index;
    std::string val;
};

namespace DataType {
    static const char kKv        = 'k';
    static const char kHash      = 'h'; // hashmap(sorted by key)
    static const char kHSize     = 'H';
    static const char kList      = 'l';
    static const char kLMeta     = 'L';
    static const char kZSet      = 'z';
    static const char kZSize     = 'Z';
    static const char kZScore    = 'Z';
//    static const char ZSET      = 's'; // key => score
//    static const char ZSCORE    = 'z'; // key|score => ""
//    static const char ZSIZE     = 'Z';
//    static const char QUEUE     = 'q';
//    static const char QSIZE     = 'Q';
}
}
#endif

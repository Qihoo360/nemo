#ifndef NEMO_INCLUDE_NEMO_CONST_H_
#define NEMO_INCLUDE_NEMO_CONST_H_

namespace nemo {

const int ZSET_SCORE_INTEGER_DIGITS = 13;
const int ZSET_SCORE_DECIMAL_DIGITS = 5;
const int64_t ZSET_SCORE_SHIFT = 1000000000000LL;
const int64_t ZSET_SCORE_MAX = ZSET_SCORE_SHIFT;
const int64_t ZSET_SCORE_MIN = -ZSET_SCORE_SHIFT;
const double eps = 1e-5;

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

struct SM {
    int64_t score;
    std::string member;
};

namespace DataType {
    static const char kKv        = 'k';
    static const char kHash      = 'h'; // hashmap(sorted by key)
    static const char kHSize     = 'H';
    static const char kList      = 'l';
    static const char kLMeta     = 'L';
    static const char kZSet      = 'z';
    static const char kZSize     = 'Z';
    static const char kZScore    = 'y';
//    static const char ZSET      = 's'; // key => score
//    static const char ZSCORE    = 'z'; // key|score => ""
//    static const char ZSIZE     = 'Z';
//    static const char QUEUE     = 'q';
//    static const char QSIZE     = 'Q';
}


}
#endif

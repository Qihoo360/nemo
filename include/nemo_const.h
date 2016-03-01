#ifndef NEMO_INCLUDE_NEMO_CONST_H_
#define NEMO_INCLUDE_NEMO_CONST_H_

namespace nemo {

const int ZSET_SCORE_INTEGER_DIGITS = 13;
const int ZSET_SCORE_DECIMAL_DIGITS = 5;
const int64_t ZSET_SCORE_SHIFT = 1000000000000000000LL;
const int64_t ZSET_SCORE_MAX = 10000000000000LL;
const int64_t ZSET_SCORE_MIN = -ZSET_SCORE_MAX;
const double eps = 1e-5;

const std::string KV_DB = "kv";
const std::string HASH_DB = "hash";
const std::string LIST_DB = "list";
const std::string ZSET_DB = "zset";
const std::string SET_DB = "set";

enum DBType {
  kKV_DB = 0,
  kHASH_DB,
  kLIST_DB,
  kZSET_DB,
  kSET_DB,
};

const uint32_t KEY_MAX_LENGTH = 255;

enum Position {
  BEFORE = 0,
  AFTER = 1
};

enum Aggregate {
  SUM = 0,
  MIN,
  MAX
};

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
    int64_t index;
    std::string val;
};

struct SM {
    double score;
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
    static const char kSet      = 's';
    static const char kSSize     = 'S';
//    static const char QUEUE     = 'q';
//    static const char QSIZE     = 'Q';
}


}
#endif

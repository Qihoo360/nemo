#ifndef NEMO_INCLUDE_NEMO_HASH_H_
#define NEMO_INCLUDE_NEMO_HASH_H_

#include "rocksdb/db.h"
#include "rocksdb/table.h"

namespace nemo {

using namespace rocksdb;

class NemoHash
{
public:
    NemoHash(const std::string &db_path, Options options);
    ~NemoHash();

};

}

#endif

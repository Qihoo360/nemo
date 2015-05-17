#ifndef NEMO_INCLUDE_NEMO_H_
#define NEMO_INCLUDE_NEMO_H_

#include <stdio.h>
#include <string>

#include "rocksdb/status.h"
#include "rocksdb/options.h"
#include "rocksdb/db.h"

namespace nemo {
using namespace rocksdb;

class Nemo
{
public:
    Nemo(const std::string &db_path, Options options);
    ~Nemo();

    Status Hset(const std::string &key, const std::string &field, const std::string &val);
    Status Hget(const std::string &key, const std::string &field, std::string *val);

private:

    std::string db_path_;
    std::unique_ptr<DB> db_;

    Nemo(const Nemo &rval);
    void operator =(const Nemo &rval);

};

}

#endif

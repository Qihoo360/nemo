#ifndef NEMO_INCLUDE_NEMO_H_
#define NEMO_INCLUDE_NEMO_H_

//#include <stdio.h>
//#include <string>

//#include "rocksdb/status.h"
//#include "rocksdb/options.h"
#include "rocksdb/db.h"
#include "nemo_iterator.h"

namespace nemo {
//using namespace rocksdb;
typedef rocksdb::Options Options;
typedef rocksdb::Status Status;
typedef rocksdb::Slice Slice;

//class nemo::KIterator;

class Nemo
{
public:
    Nemo(const std::string &db_path, Options options);
    ~Nemo();
//    Iterator *iterator(const string &start, const string &end, uint64_t limit);
//    Iterator *rev_iterator(const string &start, const string &end, uint64_t limit);

    Status Set(const std::string &key, const std::string &val);
    Status Get(const std::string &key, std::string *val);
    KIterator* scan(const Slice &start, const Slice &end, uint64_t limit);

    /*
     * The Hash function interface
     */
    Status HSet(const std::string &key, const std::string &field, const std::string &val);
    Status HGet(const std::string &key, const std::string &field, std::string *val);
    Status HKeys(const std::string &key, std::vector<std::string> *fields);


private:

    std::string db_path_;
    std::unique_ptr<rocksdb::DB> db_;

    Nemo(const Nemo &rval);
    void operator =(const Nemo &rval);

};

}

#endif

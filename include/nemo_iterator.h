#ifndef NEMO_INCLUDE_NEMO_ITERATOR_H_
#define NEMO_INCLUDE_NEMO_ITERATOR_H_
#include "rocksdb/db.h"
#include "nemo_const.h"

namespace nemo {
class Iterator{
public:
    enum Direction{
        FORWARD, BACKWARD
    };
    Iterator(rocksdb::Iterator *it,
            const std::string &end,
            uint64_t limit,
            Direction direction=Iterator::FORWARD);
    ~Iterator();
    bool skip(uint64_t offset);
    bool next();
    rocksdb::Slice key();
    rocksdb::Slice val();
private:
    rocksdb::Iterator *it;
    std::string end;
    uint64_t limit;
    bool is_first;
    int direction;
};

class KIterator{
public:
    std::string key;
    std::string val;

    KIterator(Iterator *it);
    ~KIterator();
    void return_val(bool onoff);
    bool next();
private:
    Iterator *it;
    bool return_val_;
};

class HIterator{
public:
    std::string key;
    std::string field;
    std::string val;

    HIterator(Iterator *it, const rocksdb::Slice &key);
    ~HIterator();
    void return_val(bool onoff);
    bool next();
private:
    Iterator *it;
    bool return_val_;
};
}
#endif

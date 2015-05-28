#ifndef NEMO_INCLUDE_ITERATOR_H_
#define NEMO_INCLUDE_ITERATOR_H_
#include "rocksdb/db.h"
#include "nemo_const.h"
//#include <string>
//#include "rocksdb/slice.h"

//namespace rocksdb {
//    class Iterator;
//    class Slice;
//}

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
}
#endif

#ifndef NEMO_INCLUDE_NEMO_ITERATOR_H_
#define NEMO_INCLUDE_NEMO_ITERATOR_H_
#include "rocksdb/db.h"
#include "nemo_const.h"

namespace nemo {

enum Direction {
    kForward = 0,
    kBackward = 1
};

class Iterator {
public:
    Iterator(rocksdb::Iterator *it, const std::string &end, uint64_t limit, rocksdb::ReadOptions options, Direction direction = kForward);
    ~Iterator();
    bool Skip(uint64_t offset);
    bool Next();
    bool Valid() { return it_->Valid(); };
    rocksdb::Slice Key();
    rocksdb::Slice Val();
    rocksdb::ReadOptions Opt() { return options_; };

    std::string PrevKey() {  return prev_key_; }
    std::string PrevVal() {  return prev_val_; }
    bool SetPrevKeyVal();

    void RawNext() {    it_->Next(); }
    void RawPrev() {    it_->Prev(); }
    int GetDirection() {  return direction_; }
private:
    rocksdb::Iterator *it_;
    std::string end_;
    uint64_t limit_;
    rocksdb::ReadOptions options_;
    bool is_first_;
    int direction_;
    std::string prev_key_;
    std::string prev_val_;
    //No Copying Allowed
    Iterator(Iterator&);
    void operator=(Iterator&);
};

class KIterator {
public:
    KIterator(Iterator *it);
    ~KIterator();
    bool Next();
    bool Valid() { return it_->Valid(); };
    bool Skip(int64_t offset);
    rocksdb::ReadOptions Opt() { return it_->Opt(); };
    std::string Key() { return key_; };
    std::string Val() { return val_; };
private:
    Iterator *it_;
    std::string key_;
    std::string val_;
    //No Copying Allowed
    KIterator(KIterator&);
    void operator=(KIterator&);
};

class HIterator {
public:
    HIterator(Iterator *it, const rocksdb::Slice &key);
    ~HIterator();
    bool Next();
    bool Valid();
    bool Skip(int64_t offset);
    rocksdb::ReadOptions Opt() { return it_->Opt(); };
    std::string Key() { return key_; };
    std::string Field() { return field_; };
    std::string Val() { return val_; };
private:
    Iterator *it_;
    std::string key_;
    std::string field_;
    std::string val_;
    //No Copying Allowed
    HIterator(HIterator&);
    void operator=(HIterator&);
};

class ZIterator {
public:
    ZIterator(Iterator *it, const rocksdb::Slice &key);
    ~ZIterator();
    bool Valid();
    bool Skip(int64_t offset);
    bool Next();
    rocksdb::ReadOptions Opt() { return it_->Opt(); };
    std::string Key() { return key_; };
    double Score() { return score_; };
    std::string Member() { return member_; };
private:
    Iterator *it_;
    std::string key_;
    double score_;
    std::string member_;
    //No Copying Allowed
    ZIterator(ZIterator&);
    void operator=(ZIterator&);
    
};

class ZLexIterator {
public:
    ZLexIterator(Iterator *it, const rocksdb::Slice &key);
    ~ZLexIterator();
    bool Valid() { return it_->Valid(); };
    bool Skip(int64_t offset) { if (offset < 0) {return true;} else {return it_->Skip(offset);} };
    bool Next();
    rocksdb::ReadOptions Opt() { return it_->Opt(); };
    std::string Key() { return key_; };
    std::string Member() { return member_; };
private:
    Iterator *it_;
    std::string key_;
    std::string member_;
    //No Copying Allowed
    ZLexIterator(ZLexIterator&);
    void operator=(ZLexIterator&);
    
};

class SIterator {
public:
    SIterator(Iterator *it, const rocksdb::Slice &key);
    ~SIterator();
    bool Next();
    bool Valid();
    bool Skip(int64_t offset);
    rocksdb::ReadOptions Opt() { return it_->Opt(); };
    std::string Key() { return key_; };
    std::string Member() { return member_; };
private:
    Iterator *it_;
    std::string key_;
    std::string member_;
    //No Copying Allowed
    SIterator(SIterator&);
    void operator=(SIterator&);
};

}
#endif

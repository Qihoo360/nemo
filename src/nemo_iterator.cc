#include "nemo_iterator.h"
#include "nemo_kv.h"
#include "nemo_hash.h"
#include "xdebug.h"

nemo::Iterator::Iterator(rocksdb::Iterator *it,
        const std::string &end,
        uint64_t limit,
        Direction direction)
{
    this->it = it;
    this->end = end;
    this->limit = limit;
    this->is_first = true;
    this->direction = direction;
}

nemo::Iterator::~Iterator(){
    delete it;
}

rocksdb::Slice nemo::Iterator::key(){
    rocksdb::Slice s = it->key();
    return rocksdb::Slice(s.data(), s.size());
}

rocksdb::Slice nemo::Iterator::val(){
    rocksdb::Slice s = it->value();
    return rocksdb::Slice(s.data(), s.size());
}

bool nemo::Iterator::skip(uint64_t offset){
    while(offset-- > 0){
        if(this->next() == false){
            return false;
        }
    }
    return true;
}

bool nemo::Iterator::next(){
    if(limit == 0){
        return false;
    }
    if(is_first){
        is_first = false;
    }else{
        if(direction == FORWARD){
            it->Next();
        }else{
            it->Prev();
        }
    }

    if(!it->Valid()){
        // make next() safe to be called after previous return false.
        limit = 0;
        return false;
    }
    if(direction == FORWARD){
        if(!end.empty() && it->key().compare(end) > 0){
            limit = 0;
            return false;
        }
    }else{
        if(!end.empty() && it->key().compare(end) < 0){
            limit = 0;
            return false;
        }
    }
    limit --;
    return true;
}

/***
 * KV
***/
nemo::KIterator::KIterator(Iterator *it){
    this->it = it;
    this->return_val_ = true;
}

nemo::KIterator::~KIterator(){
    delete it;
}

void nemo::KIterator::return_val(bool onoff){
    this->return_val_ = onoff;
}

bool nemo::KIterator::next(){
    while(it->next()){
        rocksdb::Slice ks = it->key();
        rocksdb::Slice vs = it->val();
        //dump(ks.data(), ks.size(), "z.next");
        //dump(vs.data(), vs.size(), "z.next");
        if(ks.data()[0] != DataType::KV){
            return false;
        }
        if(decode_kv_key(ks, &this->key) == -1){
            continue;
        }
        if(return_val_){
            this->val.assign(vs.data(), vs.size());
        }
        return true;
    }
    return  false;
}

/***
 * HASH
***/

nemo::HIterator::HIterator(Iterator *it, const rocksdb::Slice &key){
    this->it = it;
    this->key.assign(key.data(), key.size());
    this->return_val_ = true;
}

nemo::HIterator::~HIterator(){
    delete it;
}

void nemo::HIterator::return_val(bool onoff){
    this->return_val_ = onoff;
}

bool nemo::HIterator::next(){
    while(it->next()){
        rocksdb::Slice ks = it->key();
        rocksdb::Slice vs = it->val();
        //dump(ks.data(), ks.size(), "z.next");
        //dump(vs.data(), vs.size(), "z.next");
        if(ks.data()[0] != DataType::HASH){
            return false;
        }
        std::string k;
        if(decode_hash_key(ks, &k, &this->field) == -1){
            continue;
        }
        if(k != this->key){
            return false;
        }
        if(return_val_){
            this->val.assign(vs.data(), vs.size());
        }
        return true;
    }
    return false;
}

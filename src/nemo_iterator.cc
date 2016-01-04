#include "nemo_iterator.h"

#include <iostream>
#include "nemo_set.h"
#include "nemo_hash.h"
#include "nemo_zset.h"
#include "xdebug.h"

nemo::Iterator::Iterator(rocksdb::Iterator *it, const IteratorOptions& iter_options)
  : it_(it),
    ioptions_(iter_options) {
      Check();
    }

bool nemo::Iterator::Check() {
  valid_ = false;
  if (ioptions_.limit == 0 || !it_->Valid()) {
    // make next() safe to be called after previous return false.
    ioptions_.limit = 0;
    return false;
  } else {
    if (ioptions_.direction == kForward) {
      if (!ioptions_.end.empty() && it_->key().compare(ioptions_.end) > 0) {
        ioptions_.limit = 0;
        return false;
      }
    } else {
      if(!ioptions_.end.empty() && it_->key().compare(ioptions_.end) < 0) {
        ioptions_.limit = 0;
        return false;
      }
    }
    ioptions_.limit --;
    valid_ = true;
    return true;
  }
}

rocksdb::Slice nemo::Iterator::key() {
  return it_->key();
}

rocksdb::Slice nemo::Iterator::value() {
  return it_->value();
}

bool nemo::Iterator::Valid() {
  if (valid_) {
    Check();
  }
  return valid_;
}

void nemo::Iterator::Skip(int64_t offset) {
  if (offset < 0) {
    valid_ = false;
  } else {
    while (offset-- > 0) {
      if (ioptions_.direction == kForward){
        it_->Next();
      } else {
        it_->Prev();
      }

      if (!Check()) {
        return;
      }
    }
  }
}

void nemo::Iterator::Next() {
  if (valid_) {
    if (ioptions_.direction == kForward){
      it_->Next();
    } else {
      it_->Prev();
    }
    
    Check();
  }
}

// KV
void nemo::KIterator::LoadData() {
  rocksdb::Slice ks = Iterator::key();
  rocksdb::Slice vs = Iterator::value();
  this->key_.assign(ks.data(), ks.size());
  this->value_.assign(vs.data(), vs.size());
}

void nemo::KIterator::Next() {
  Iterator::Next();
  if (valid_) {
    LoadData();
  }
}

void nemo::KIterator::Skip(int64_t offset) {
  Iterator::Skip(offset);
  if (valid_) {
    LoadData();
  }
}

// HASH
nemo::HIterator::HIterator(rocksdb::Iterator *it, const IteratorOptions iter_options, const rocksdb::Slice &key)
  : Iterator(it, iter_options) {
    this->key_.assign(key.data(), key.size());
  }

// check valid and load field_, value_
bool nemo::HIterator::Valid() {
  if (valid_) {
    rocksdb::Slice ks = Iterator::key();

    if (ks[0] == DataType::kHash) {
      std::string k;
      if (DecodeHashKey(ks, &k, &this->field_) != -1) {
        if (k == this->key_) {
          rocksdb::Slice vs = Iterator::value();
          this->value_.assign(vs.data(), vs.size());
          return true;
        }
      }
    }
  }
  valid_ = false;
  return false;
}

void nemo::HIterator::Next() {
  Iterator::Next();
  Valid();
}

void nemo::HIterator::Skip(int64_t offset) {
  Iterator::Skip(offset);
  Valid();
}


// ZSET
nemo::ZIterator::ZIterator(rocksdb::Iterator *it, const IteratorOptions iter_options, const rocksdb::Slice &key)
  : Iterator(it, iter_options) {
    this->key_.assign(key.data(), key.size());
  }

// check valid and assign member_ and score_
bool nemo::ZIterator::Valid() {
  if (valid_) {
    rocksdb::Slice ks = Iterator::key();
    if (ks[0] == DataType::kZScore) {
      std::string k;
      if (DecodeZScoreKey(ks, &k, &this->member_, &this->score_) != -1) {
        if (k == this->key_) {
          return true;
        }
      }
    }
  }
  valid_ = false;
  return false;
}

void nemo::ZIterator::Next() {
  Iterator::Next();
  Valid();
}

void nemo::ZIterator::Skip(int64_t offset) {
  Iterator::Skip(offset);
  Valid();
}

// ZLexIterator
nemo::ZLexIterator::ZLexIterator(rocksdb::Iterator *it, const IteratorOptions iter_options, const rocksdb::Slice &key)
  : Iterator(it, iter_options) {
    this->key_.assign(key.data(), key.size());
  }

bool nemo::ZLexIterator::Valid() {
  if (valid_) {
    rocksdb::Slice ks = Iterator::key();

    if (ks[0] == DataType::kZSet) {
      std::string k;
      if (DecodeZSetKey(ks, &k, &this->member_) != -1) {
        if (k == this->key_) {
          return true;
        }
      }
    }
  }
  valid_ = false;
  return false;
}

void nemo::ZLexIterator::Next() {
  Iterator::Next();
  Valid();
}

void nemo::ZLexIterator::Skip(int64_t offset) {
  Iterator::Skip(offset);
  Valid();
}

// SET
nemo::SIterator::SIterator(rocksdb::Iterator *it, const IteratorOptions iter_options, const rocksdb::Slice &key)
  : Iterator(it, iter_options) {
    this->key_.assign(key.data(), key.size());
  }

// check valid and assign member_
bool nemo::SIterator::Valid() {
  if (valid_) {
    rocksdb::Slice ks = Iterator::key();

    if (ks[0] == DataType::kSet) {
      std::string k;
      if (DecodeSetKey(ks, &k, &this->member_) != -1) {
        if (k != this->key_) {
          return true;
        }
      }
    }
  }
  valid_ = false;
  return false;
}

void nemo::SIterator::Next() {
  Iterator::Next();
  Valid();
}

void nemo::SIterator::Skip(int64_t offset) {
  Iterator::Skip(offset);
  Valid();
}

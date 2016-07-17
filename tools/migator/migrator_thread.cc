#include "migrator_thread.h"

MigratorThread::~MigratorThread() {
}


void MigratorThread::MigrateDB(char type) {
  if (type == nemo::DataType::kKv) {
    std::string dummy_key = "";
    DispatchKey(dummy_key, type); 
  } else {
    rocksdb::Iterator *keyIter = db_->KeyIterator(type);
    for(; keyIter->Valid(); keyIter->Next()) {
      std::string key = GetKey(keyIter);
      if (key.length() == 0) break;

      DispatchKey(key, type);
    }
  }
}

std::string  MigratorThread::GetKey(const rocksdb::Iterator *it) {
  return it->key().ToString().substr(1);
}

void MigratorThread::DispatchKey(const std::string &key,char type) {
  parsers_[thread_index_]->Schedul(key, type);
  thread_index_ = (thread_index_ + 1) % num_thread_;
}

void *MigratorThread::ThreadMain() {
  MigrateDB(type_); 
  return NULL;
}



#include "migrator_thread.h"

MigratorThread::~MigratorThread() {
}


void MigratorThread::MigrateDB(char type) {
  if (type == nemo::DataType::kKv) {
    nemo::KIterator *iter = db_->KScan("", "", -1, false);
    // SET <key> <vaule> [EX SEC]
    for (; iter->Valid(); iter->Next()) {
      pink::RedisCmdArgsType argv;
      std::string cmd;
     
      std::string key = iter->key();
      int64_t ttl; 
      db_->KTTL(key, &ttl);

      argv.push_back("SET");
      argv.push_back(key);
      argv.push_back(iter->value());

      if (ttl > 0) {
        argv.push_back("EX");
        argv.push_back(std::to_string(ttl));
      }

      pink::RedisCli::SerializeCommand(argv, &cmd);
      DispatchKey(cmd, type);
    }
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
  should_exit_ = true;
  std::cout << type_ << " keys have been dispatched completly" << std::endl;
  return NULL;
}



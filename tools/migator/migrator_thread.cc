#include "migrator_thread.h"

MigratorThread::~MigratorThread() {
}


void MigratorThread::MigrateDB(char type) {
  if (type == nemo::DataType::kKv) {
    nemo::KIterator *iter = db_->KScan("", "", -1, false);
    for (; iter->Valid(); iter->Next()) {
      pink::RedisCmdArgsType argv;
      std::string cmd;

      argv.push_back("SET");
      argv.push_back(iter->key());
      argv.push_back(iter->value());

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
  std::cout << type_ << "keys have been dispatched completly" << std::endl;
  return NULL;
}



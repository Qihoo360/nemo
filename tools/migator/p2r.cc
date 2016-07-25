#include <iostream>
#include <string>
#include <sstream>
#include "redis_cli.h"
#include "nemo.h"
#include "parse_thread.h"
#include "sender_thread.h"
#include "migrator_thread.h"

const int64_t kTestPoint = 500000;
// const int64_t kTestPoint = 5000;
// const int64_t kTestNum = 1000000;
const int64_t kTestNum = LLONG_MAX;

const size_t num_thread = 10; //
size_t thread_index = 0;

std::vector<ParseThread*> parsers;
std::vector<SenderThread*> senders;
std::vector<MigratorThread*> migrators;
nemo::Nemo *db;

std::string GetKey(const rocksdb::Iterator *it);
void MigrateDB(char type);
void DispatchKey(const std::string &key, char type);
void StopThreads();
int64_t GetNum(); 

void Usage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "./p2r db_path ip port" << std::endl;
}

int64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

int main(int argc, char **argv)
{
  // for coding test
  // std::string db_path = "/home/yinshucheng/pika/output/mydb/";
  std::string db_path = "/home/yinshucheng/test/db";
  std::string ip = "127.0.0.1";
  int port = 6379;

  if (argc != 1) {
    if (argc == 4) {
      db_path = std::string(argv[1]);
      ip = std::string(argv[2]);
      port = atoi(argv[3]);
    } else {
      Usage();
      return -1;
    }
  }
  // init db
  nemo::Options option;
  option.write_buffer_size = 256 * 1024 * 1024; // 256M
  option.target_file_size_base = 20 * 1024 * 1024; // 20M
  db = new nemo::Nemo(db_path ,option);

  // init ParseThread and SenderThread 
  pink::Status pink_s;
  for (size_t i = 0; i < num_thread; i++) {
     // init a redis-cli
    pink::RedisCli *cli = new pink::RedisCli();
    cli->set_connect_timeout(3000);
    pink_s = cli->Connect(ip, port);
    if(!pink_s.ok()) {
      Usage();
      log_err("cann't connect %s:%d:%s\n", ip.data(), port, pink_s.ToString().data());
      return -1;
    }
    SenderThread *sender = new SenderThread(cli);

    senders.push_back(sender);
    parsers.push_back(new ParseThread(db, sender));
  }

  migrators.push_back(new MigratorThread(db, parsers, nemo::DataType::kKv));
  migrators.push_back(new MigratorThread(db, parsers, nemo::DataType::kHSize));       
  migrators.push_back(new MigratorThread(db, parsers, nemo::DataType::kSSize));
  migrators.push_back(new MigratorThread(db, parsers, nemo::DataType::kLMeta));
  migrators.push_back(new MigratorThread(db, parsers, nemo::DataType::kZSize));

  for (size_t i = 0; i < migrators.size(); i++) {
    migrators[i]->StartThread();
  }

  // start threads
  for (size_t i = 0; i < num_thread; i++) {
    parsers[i]->StartThread();
    senders[i]->StartThread();
  }

  int64_t start_time = NowMicros();
  int times = 1;
  while(1) {
    sleep(1);
    int64_t num = GetNum();
    if (num >= kTestPoint * times) {
      times++;
      int dur = NowMicros() - start_time;

      std::cout << dur / 1000000 << ":" << dur % 1000000 << " " <<  num << std::endl; 
      log_info("timestamp:%d:%d line:%ld", dur / 1000000, dur % 1000000, num);
    }

    bool should_exit = true;
    for (size_t i = 0; i < migrators.size(); i++) {
      if (!migrators[i]->should_exit_) {
        should_exit = false;
        break; 
      }
    }

    if (num >= kTestNum) {
      should_exit = true;
    } 
    
    if (should_exit) {
      for (size_t i = 0; i < num_thread; i++) {
        parsers[i]->should_exit_ = true;
      } 
      break;
    }
  }

  for (size_t i = 0; i < num_thread; i++) {
    parsers[i]->JoinThread();
    senders[i]->JoinThread();
  }

  for (size_t i = 0; i < migrators.size(); i++) {
    delete migrators[i];
  }
  for (size_t i = 0; i < num_thread; i++) {
    delete parsers[i];
    delete senders[i];
  }

  delete db;

  return 0;
}

int64_t GetNum() {
  int64_t num = 0;
  for (size_t i = 0; i < num_thread; i++) {
    num += parsers[i]->num();
  }
  return num;
}



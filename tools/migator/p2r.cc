#include <iostream>
#include <string>
#include <sstream>
#include "redis_cli.h"
#include "nemo.h"
#include "parse_thread.h"
#include "sender_thread.h"

const int64_t kTestPoint = 100000;
const int64_t kTestNum = 3800000;
int times = 1; 

const int num_thread = 10; // const int64_t kTestNum = ULLONG_MAX;

int thread_index = 0;


class FunctionTimer {
public:
  void Start() {
    gettimeofday(&tv_start, NULL);
  }

  void Click() {
    gettimeofday(&tv_end, NULL);
    micorsec += (tv_end.tv_sec - tv_start.tv_sec) * 1000000
        + (tv_end.tv_usec - tv_start.tv_usec);
  }

  void End() {
    gettimeofday(&tv_end, NULL);
  }

  int TotalTime() {
    return micorsec / 1000000;
  }

private:
  struct timeval tv_start;
  struct timeval tv_end;
  float micorsec;
};


std::vector<ParseThread*> parsers;
std::vector<SenderThread*> senders;
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

int main(int argc, char **argv)
{
  // for coding test
  // string db_path = "/home/yinshucheng/pika/output/db/";
  std::string db_path = "/home/yinshucheng/db2/";
  std::string ip = "127.0.0.1";
  int port = 6379;

  if(argc == 4) {
    db_path = std::string(argv[1]);
    ip = std::string(argv[2]);
    port = atoi(argv[3]);
  }

  // for release
  // if (argc != 4) {
    // Usage();
    // log_err("");
  // }
  // string db_path(argv[1]);
  // string ip(argv[2]);
  // int port = atoi(argv[3]);

  // init db
  nemo::Options option;
  option.write_buffer_size = 256 * 1024 * 1024; // 256M
  option.target_file_size_base = 20 * 1024 * 1024; // 20M
  db = new nemo::Nemo(db_path ,option);

  pink::Status pink_s;

  // init ParseThread and SenderThread 
  for (int i = 0; i < num_thread; i++) {
     // init a redis-cli
    pink::RedisCli *cli = new pink::RedisCli();
    cli->set_connect_timeout(3000);
    pink_s = cli->Connect(ip, port);
    if(!pink_s.ok()) {
      log_err("cann't connect %s:%d:%s", ip.data(), port, pink_s.ToString().data());
      Usage();
      return -1;
    }
    SenderThread *sender = new SenderThread(cli);

    senders.push_back(sender);
    parsers.push_back(new ParseThread(db, sender));
  }

  // start threads
  for (int i = 0; i < num_thread; i++) {
    parsers[i]->StartThread();
    senders[i]->StartThread();
  }

  MigrateDB(nemo::DataType::kHSize);

  delete db;

  return 0;
}

void MigrateDB(char type) {
  FunctionTimer timer;
  timer.Start();
  if (type == nemo::DataType::kKv) {
    std::string dummy_key = "";
    DispatchKey(dummy_key, type); 
  } else {
    rocksdb::Iterator *keyIter = db->KeyIterator(type);
    for(; keyIter->Valid(); keyIter->Next()) {
      std::string key = GetKey(keyIter);
      if (key.length() == 0) break;

      DispatchKey(key, type);
      
      int64_t num = GetNum();
      if (num >= kTestPoint * times) {
        times++;
        log_info("has send %lu", num);
      }
      if (num >= kTestNum) {
        StopThreads();
      }
    }
  }
  timer.End();
  log_info("DataType:%c Scan&Dispatch %d",type,  timer.TotalTime());
}

std::string GetKey(const rocksdb::Iterator *it) {
  return it->key().ToString().substr(1);
}

void DispatchKey(const std::string &key,char type) {
  parsers[thread_index]->Schedul(key, type);
  thread_index = (thread_index + 1) % num_thread;
}

void StopThreads() {
  for (int i = 0; i < num_thread; i++) {
    // parsers[i]->Stop();
  }
}

int64_t GetNum() {
  int64_t num = 0;
  for (int i = 0; i < num_thread; i++) {
    num += parsers[i]->num();
  }
  return num;
}



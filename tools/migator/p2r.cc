#include <iostream>
#include <string>
#include <sstream>
#include "redis_cli.h"
#include "nemo.h"
#include "parse_thread.h"
#include "sender_thread.h"
#include "migrator_thread.h"

const int64_t kTestPoint = 100000;
const int64_t kTestNum = 3800000;

const size_t num_thread = 10; // const int64_t kTestNum = ULLONG_MAX;

size_t thread_index = 0;


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

int main(int argc, char **argv)
{
  // for coding test
  // std::string db_path = "/home/yinshucheng/pika/output/mydb/";
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

  // init ParseThread and SenderThread 
  pink::Status pink_s;
  for (size_t i = 0; i < num_thread; i++) {
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

  int times = 1;
  while(1) {
    sleep(1);
    int64_t num = GetNum();
    if (num >= kTestPoint * times) {
      times++;

      std::cout << "migrate " << num;
      std::cout << " record" << std::endl;
    }
  }

  for (size_t i = 0; i < migrators.size(); i++) {
    migrators[i]->JoinThread();
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



#include <iostream>
#include <string>
#include <sstream>
#include <sys/poll.h>
#include "redis_cli.h"
#include "nemo.h"
#include "nemo_const.h"

#include "worker_thread.h"

const int64_t kTestPoint = 100000;
const int64_t kTestNum = 3800000;
// const int64_t kTestNum = ULLONG_MAX;

int fd;
int buf_index = 0;
int thread_index = 0;
const int NUM_WORKER = 2;
//
// int num = 0;

pink::RedisCli *cli;
nemo::Nemo *db;
std::vector<WorkerThread*> workers;

std::string GetKey(const rocksdb::Iterator *it);

void MigrateDB(char type);
void DispatchKey(const std::string &key, char type);
void StopWorkers();

void DealKey(const std::string &key,char type);
void DealHKey(const std::string &key);
void DealSKey(const std::string &key);
void DealCmd(const std::string &cmd);

std::string GetKey(const rocksdb::Iterator *it) {
  return it->key().ToString().substr(1);
}

void DispatchKey(const std::string &key,char type) {
  workers[thread_index]->Schedul(key, type);
  thread_index = (thread_index + 1) % NUM_WORKER;
  log_info("thread_index : %d", thread_index);
}

void StopWorkers() {
  for (int i = 0; i < NUM_WORKER; i++) {
    workers[i]->Stop();
  }
}

void MigrateDB(char type) {
  rocksdb::Iterator *keyIter = db->KeyIterator(type);
  for(; keyIter->Valid(); keyIter->Next()) {
    std::string key = GetKey(keyIter);
    if(key.length() == 0) break;

    // stop program
    // if(num >= kTestNum) {
    //   StopWorkers();
    //   return;
    // }
    DispatchKey(key, type);
    log_info("DispatchKey");
  }
}


void Usage() {
  // std::cout << "Usage: " << std::endl;
  // std::cout << "./p2r db_path ip port" << std::endl;
}

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


int main(int argc, char **argv)
{
  // for coding
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


  // init WorkerThread and init buf
  for (int i = 0; i < NUM_WORKER; i++) {
     // init redis-cli
    pink::RedisCli *cli;
    cli = new pink::RedisCli();
    cli->set_connect_timeout(3000);
    pink::Status pink_s;
    pink_s = cli->Connect(ip, port);
    if(!pink_s.ok()) {
      log_err("cann't connect %s:%d:%s", ip.data(), port, pink_s.ToString().data());
     /*  std::cout << pink_s.ToString() << std::endl; */
      // std::cout << "ip : " << ip << std::endl;
      /* std::cout << "port : " << port << std::endl; */
    return -1;
    }
    
    workers.push_back(new WorkerThread(db, cli));
  }

  // start threads
  for (int i = 0; i < NUM_WORKER; i++) {
    workers[i]->StartThread();
  }

  MigrateDB(nemo::DataType::kHSize);

  log_info("over");
  // pthread_t consumer;
  // pthread_create(&consumer, NULL, PipeMode, NULL);

  delete db;

  return 0;
}

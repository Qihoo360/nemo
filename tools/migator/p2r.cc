#include <iostream>
#include <string>
#include <sstream>
#include <sys/poll.h>
#include "redis_cli.h"
#include "nemo.h"
#include "nemo_const.h"

const int64_t kTestPoint = 100000;
const int64_t kTestNum = 3800000;
// const int64_t kTestNum = ULLONG_MAX;

int fd;
int buf_index = 0;
int thread_index = 0;
const int NUM_WORKER = 10;
const int WRITE_LOOP_MAX_BYTES = 128 * 1024;
int num = 0;

pink::RedisCli *cli;
nemo::Nemo *db;

void DealKey(const std::string &key,char type); 
void DealHKey(const std::string &key);
void DealSKey(const std::string &key);
void DealCmd(const std::string &cmd);

class WorkerThread {
public:
  WorkerThread(int full = 1000) :
      full_(full){
    pthread_mutex_init(&mu_, NULL);
    pthread_cond_init(&rsignal_, NULL);
    pthread_cond_init(&wsignal_, NULL);
      }

  static void *RunThread(void *arg);
  void StartThread();
  void Schedul (const std::string &key, char type);
  void *ThreadMain();
  void Stop ();
  pthread_t thread_id();
  
private:
  struct Task {
    std::string key;
    char type;
    Task(std::string _key, char _type)
      : key(_key), type(_type) {}
  };

  std::atomic<bool> should_exit_;
  std::atomic<bool> running_;
  std::deque<Task> task_queue_;
  pthread_t thread_id_;
  pthread_mutex_t mu_;
  pthread_cond_t rsignal_;
  pthread_cond_t wsignal_;
  int full_;
};

pthread_t WorkerThread::thread_id() {
  return thread_id_;
}

void *WorkerThread::RunThread(void *arg)
{
  reinterpret_cast<WorkerThread*>(arg)->ThreadMain();
  return NULL;
}

void WorkerThread::StartThread()
{
  should_exit_ = false;
  pthread_create(&thread_id_, NULL, RunThread, (void *)this);
}

void WorkerThread::Schedul(const std::string &key, char type) {
  // log_info("int schedul full = %d", full_); 
  pthread_mutex_lock(&mu_);
  // log_info("lock");
  while(task_queue_.size() >= full_) {
    pthread_cond_wait(&wsignal_, &mu_);
    
  }

  // log_info("not full");
  if(task_queue_.size() < full_) {
    task_queue_.push_back(Task(key, type));
    pthread_cond_signal(&rsignal_);
  }
  pthread_mutex_unlock(&mu_);
  // log_info("out schedul");
}

void *WorkerThread::ThreadMain() {
  while (!should_exit_) {
    // pthread_mutex_lock(&mu_);
    while (task_queue_.empty() && !should_exit_) {
      // log_info("wait");
      pthread_cond_wait(&rsignal_, &mu_);
    }
    // log_info("start");
    if (should_exit_) {
      pthread_mutex_unlock(&mu_);
      continue;
    }
    std::string key = task_queue_.front().key;
    char type = task_queue_.front().type;
    task_queue_.pop_front();
    pthread_cond_signal(&wsignal_);
    DealKey(key, type);
  }
  return NULL;
}

void WorkerThread::Stop () {
  should_exit_ = true;
  if (running_) {
    pthread_cond_signal(&rsignal_);
    pthread_cond_signal(&wsignal_);
    running_ = false;
  }
}


std::vector<WorkerThread*> workers;
std::map<pthread_t, char*> cmdbufs;

std::string GetKey(const rocksdb::Iterator *it) {
  return it->key().ToString().substr(1);
}

void StopWorkers() {
  for (int i = 0; i < NUM_WORKER; i++) {
    workers[i]->Stop();
  }
}


void DispatchKey(const std::string &key,char type) {
  workers[thread_index]->Schedul(key, type);
  thread_index = (thread_index + 1) % NUM_WORKER;
}

void MigrateDB(char type) {
  rocksdb::Iterator *keyIter = db->KeyIterator(type);
  for(; keyIter->Valid(); keyIter->Next()) {
    std::string key = GetKey(keyIter);
    if(key.length() == 0) break;

    // stop program
    if(num >= kTestNum) {
      StopWorkers();
      return;
    }
    DispatchKey(key, type);

  }
}

void DealKey(const std::string &key,char type) {
  if (type == nemo::DataType::kHSize) {
    DealHKey(key);
  } else if (type == nemo::DataType::kSSize) {
    DealSKey(key);
  }
}

void DealHKey(const std::string &key) {
  nemo::HIterator *iter = db->HScan(key, "", "", -1, false);
  for (; iter->Valid(); iter->Next()) {
    pink::RedisCmdArgsType argv;
    std::string cmd;

    argv.push_back("HSET");
    argv.push_back(iter->key());
    argv.push_back(iter->field());
    argv.push_back(iter->value());

    pink::RedisCli::SerializeCommand(argv, &cmd);
    DealCmd(cmd);
  }
  delete iter;
}

void DealSKey(const std::string &key) {
  nemo::SIterator *iter = db->SScan(key, -1, false);
  for (; iter->Valid(); iter->Next()) {
    pink::RedisCmdArgsType argv;
    std::string cmd;

    argv.push_back("SADD");
    argv.push_back(iter->key());
    argv.push_back(iter->member());

    pink::RedisCli::SerializeCommand(argv, &cmd);
    DealCmd(cmd);
  }
  delete iter;
}

void DealCmd(const std::string &cmd) {
  // char buf[] = cmdbufs[pthread_self()];
  num++;
  std::cout << cmd;
  if (num %  kTestPoint == 0) {
    log_info("deal cmd %d" , num);
  }
  
  // xxi << std::endl;

}

void Usage() {
  // std::cout << "Usage: " << std::endl;
  // std::cout << "./p2r db_path ip port" << std::endl;
}
// const int kReadable = 1;
// const int kWritable = 2;
// int Wait(int fd, int mask, long long milliseconds) {
  // struct pollfd pfd;
  // int retmask = 0;
  // int retval;

  // memset(&pfd, 0, sizeof(pfd));
  // pfd.fd = fd;
  // if (mask & kReadable) pfd.events |= POLLIN;
  // if (mask & kWritable) pfd.events |= POLLOUT;

  // if((retval = poll(&pfd, 1, milliseconds)) == 1) {
    // if (pfd.revents & POLLIN) retmask |= kReadable;
    // if (pfd.revents & POLLOUT) retmask |= kWritable;
    // if (pfd.revents & POLLERR) retmask |= kWritable;
    // if (pfd.revents & POLLHUP) retmask |= kWritable;
    // return retmask;
  // } else {
    // return retval;
  // }
// }

// void SetFlag(int fd, int flags) {
  // int old_flags = fcntl(fd, F_GETFL, 0);

// }

// int GetFlags(int fd) {
  // return fcntl(fd,F_GETFL);
// }

// void *PipeMode(void*) {
  // int done = 0;
  // int flags;
  // int eof = 0;
  // if ((flags = fcntl(socket_fd, F_GETFL)) == -1) {
    // log_err("fcntl(F_GETFL): %s", strerror(errno));
  // }

  // flags |= O_NONBLOCK;
  // if (fcntl(socket_fd, F_SETFL, flags) == -1) {
    // log_err("fcntl(F_SETFL,O_NONBLOCK): %s", strerror(errno));
  // }

  // int fd = socket_fd;
  // while(!done) {
    // int mask = kReadable;
    // if(!eof || obuf_len != 0) mask |= kWritable;

    // mask = Wait(fd, mask, 1000);

    // if(mask & kReadable) {
      // cli->Recv(NULL);
    // }

    // if(mask & kWritable) {
      // int loop_nwritten = 0;

      // while(1) {
        // if(obuf_len != 0) {
          // int nwritten = write(fd, obuf+obuf_pos, obuf_len);
          // if(nwritten == -1) {
            // if(errno != EAGAIN && errno != EINTR) {
              // log_err("Error writing to the server : %s", strerror(errno));
              // return NULL;
            // } else {
              // nwritten = 0;
            // }
          // }
          // obuf_len -= nwritten;
          // obuf_pos += nwritten;
          // loop_nwritten += nwritten;
          // if(obuf_len != 0) break;
        // }
        // // load from pipe, if buffer is empty
        // if(obuf_len == 0 && !eof) {
          // int nread = read(receive_fd, obuf, sizeof(obuf));

          // if(nread == 0) {
           // [>  char echo[] =  <]
                // // " */\r\n*2\r\n$4\r\nECHO\r\n$20\r\n01234567890123456789\r\n";
            // done = 1;
            // eof = 1;

          // } else if (nread == -1) {
            // log_err("Error reading from pipe: %s", strerror(errno));
          // } else {
            // obuf_len = nread;
            // obuf_pos = 0;
          // }
        // }

        // // no data to read from pipe or write too much data
        // if((obuf_len == 0 && eof) ||
           // loop_nwritten > WRITE_LOOP_MAX_BYTES) {
          // // log_info("no deta from pipe");
          // break;
        // }
      // }
    // }
  // }
  // log_info("over in consumer");
  // return NULL;
// }


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

  // init redis-cli
  cli = new pink::RedisCli();
  cli->set_connect_timeout(3000);
  pink::Status pink_s;
  pink_s = cli->Connect(ip, port);
  if(!pink_s.ok()) {
   /*  std::cout << pink_s.ToString() << std::endl; */
    // std::cout << "ip : " << ip << std::endl;
    /* std::cout << "port : " << port << std::endl; */
    return -1;
  }
  fd = cli->fd();

  // init WorkerThread and init buf
  for (int i = 0; i < NUM_WORKER; i++) {
    workers.push_back(new WorkerThread());
    char buf[1024*16];
    cmdbufs[workers[i]->thread_id()] = buf;
  }
  // start threads
  for (int i = 0; i < NUM_WORKER; i++) {
    workers[i]->StartThread();
  }

  MigrateDB(nemo::DataType::kHSize);
  
  // pthread_t consumer;
  // pthread_create(&consumer, NULL, PipeMode, NULL);

  delete cli;
  delete db;

  return 0;
}



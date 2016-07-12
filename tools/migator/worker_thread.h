#ifndef WORKER_THREAD_H_
#define WORKER_THREAD_H_

#include <sys/poll.h>
#include "redis_cli.h"
#include "nemo.h"

class WorkerThread {
public:
  WorkerThread(nemo::Nemo *db, pink::RedisCli *cli, int full = 100) :
    db_(db), cli_(cli), full_(full), num_(0) {
      
      pthread_mutex_init(&mu_, NULL);
      pthread_cond_init(&rsignal_, NULL);
      pthread_cond_init(&wsignal_, NULL);

      pthread_mutex_init(&bufmu_, NULL);
      pthread_cond_init(&bufrsignal_, NULL);
      pthread_cond_init(&bufwsignal_, NULL);
    }

  int64_t num() {
    return num_;
  }

  static void *RunThread(void *arg);
  static void *RunSenderThread(void *arg);
  void StartThread();
  void Schedul(const std::string &key, char type);
  void *ThreadMain();
  void Stop();
  pthread_t thread_id();

private:
  // prase key and generate cmd
  void DealKey(const std::string &key,char type);
  void DealHKey(const std::string &key);
  void DealSKey(const std::string &key);
  void DealCmd(const std::string &cmd);

  // send cmd
  const int kReadable = 1;
  const int kWritable = 2;
  int Wait(int fd, int mask, long long milliseconds);
  void *PipeMode();

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

  nemo::Nemo *db_;
  pink::RedisCli *cli_;

  char obuf[1024 * 16];
  int obuf_len = 0;
  int obuf_pos = 0;
  
  const int WRITE_LOOP_MAX_BYTES = 128 * 1024;

  pthread_t thread_sender_;
  pthread_mutex_t bufmu_;
  pthread_cond_t bufrsignal_;
  pthread_cond_t bufwsignal_;

  unsigned long full_;
  int64_t num_;
};
#endif

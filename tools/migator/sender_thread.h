#ifndef SENDER_THREAD_H_
#define SENDER_THREAD_H_

#include <sys/poll.h>
#include "redis_cli.h"
#include "pink_thread.h"
#include "pink_mutex.h"
#include "nemo.h"

class SenderThread : public pink::Thread{
public:
  SenderThread(pink::RedisCli *cli);
  ~SenderThread();
  void LoadCmd(const std::string &cmd);

private:
  static const int kReadable = 1;
  static const int kWritable = 2;
  static const size_t kBufSize = 1024 * 128;
  static const size_t kThreshold = 1024 * 64;
  static const size_t kWirteLoopMaxBYTES = 128 * 1024;
  
  int Wait(int fd, int mask, long long milliseconds);

  pink::RedisCli *cli_;
  
  char buf_[kBufSize];
  size_t buf_len_ = 0;
  size_t buf_pos_ = 0;

  pink::Mutex buf_mutex_;
  pink::CondVar buf_r_cond_;
  pink::CondVar buf_w_cond_;

  virtual void *ThreadMain();
};
#endif

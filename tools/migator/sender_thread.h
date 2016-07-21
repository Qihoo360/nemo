#ifndef SENDER_THREAD_H_
#define SENDER_THREAD_H_

#include <sys/poll.h>
#include <iostream>
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
  static const size_t kBufSize = 2097152; //2M 
  static const size_t kWirteLoopMaxBYTES = 1024 * 2048; // 10k cmds 
  
  int Wait(int fd, int mask, long long milliseconds);

  pink::RedisCli *cli_;
  char buf_[kBufSize];
  size_t buf_len_ = 0;
  size_t buf_pos_ = 0;

  char rbuf_[kBufSize];
  int32_t rbuf_size_ = kBufSize;
  int32_t rbuf_pos_;
  int32_t rbuf_offset_;
  int elements_;    // the elements number of this current reply
  int err_;

  // int GetReply();
  int GetReplyFromReader();
  char* ReadBytes(unsigned int bytes);

  int TryRead(); 
  // static char *seekNewline(char *s, size_t len);
  char *seekNewline(char *s, size_t len);
  ssize_t BufferRead();
  char* ReadLine(int *_len);


  pink::Mutex buf_mutex_;
  pink::CondVar buf_r_cond_;
  pink::CondVar buf_w_cond_;

  pink::Mutex num_mutex_;
  size_t num_;

  void PlusNum() {
    pink::MutexLock l(&num_mutex_);
    num_++;
  }

  size_t num() {
    pink::MutexLock l(&num_mutex_);
    return num_;
  }

  void DecNum() {
    pink::MutexLock l(&num_mutex_);
    num_--;
  }

  virtual void *ThreadMain();
};
#endif

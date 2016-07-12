#include "worker_thread.h"

pthread_t WorkerThread::thread_id() {
  return thread_id_;
}

void *WorkerThread::RunThread(void *arg)
{
  reinterpret_cast<WorkerThread*>(arg)->ThreadMain();
  return NULL;
}

void *WorkerThread::RunSenderThread(void *arg)
{
  reinterpret_cast<WorkerThread*>(arg)->PipeMode();
  return NULL;
}

void WorkerThread::StartThread()
{
  should_exit_ = false;
  pthread_create(&thread_id_, NULL, RunThread, (void *)this);
  pthread_create(&thread_sender_, NULL, RunSenderThread, (void *)this);
}

void WorkerThread::Schedul(const std::string &key, char type) {
  // log_info("int schedul full = %d", full_);
  pthread_mutex_lock(&mu_);
  // log_info("Main lock");
  // log_info("lock");
  while(task_queue_.size() >= full_) {
    // log_info("to many key ,wait");
    pthread_cond_wait(&wsignal_, &mu_);
    // log_info("less key ,restart");
  }

  // log_info("not full");
  if(task_queue_.size() < full_) {
    task_queue_.push_back(Task(key, type));
    pthread_cond_signal(&rsignal_);
  }
  pthread_mutex_unlock(&mu_);
  // log_info("Main unlock");
  // log_info("out schedul");
}

void *WorkerThread::ThreadMain() {
  while (!should_exit_) {
    pthread_mutex_lock(&mu_);
    while (task_queue_.empty() && !should_exit_) {
      // log_info("no key to prase");
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
    // log_info("notify Main to add key to queue");
    DealKey(key, type);
    pthread_mutex_unlock(&mu_);
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

void WorkerThread::DealKey(const std::string &key,char type) {
  if (type == nemo::DataType::kHSize) {
    DealHKey(key);
  } else if (type == nemo::DataType::kSSize) {
    DealSKey(key);
  }
}

void WorkerThread::DealHKey(const std::string &key) {
  nemo::HIterator *iter = db_->HScan(key, "", "", -1, false);
  for (; iter->Valid(); iter->Next()) {
    pink::RedisCmdArgsType argv;
    std::string cmd;

    argv.push_back("HSET");
    argv.push_back(iter->key());
    argv.push_back(iter->field());
    argv.push_back(iter->value());

    pink::RedisCli::SerializeCommand(argv, &cmd);
    DealCmd(cmd);
    /* num++; */
    // if(num % 10000 == 0) {
      // log_info("has send %d", num);
    /* } */
  }
  delete iter;
}

void WorkerThread::DealSKey(const std::string &key) {
  nemo::SIterator *iter = db_->SScan(key, -1, false);
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

int WorkerThread::Wait(int fd, int mask, long long milliseconds) {
  struct pollfd pfd;
  int retmask = 0;
  int retval;

  memset(&pfd, 0, sizeof(pfd));
  pfd.fd = fd;
  if (mask & kReadable) pfd.events |= POLLIN;
  if (mask & kWritable) pfd.events |= POLLOUT;

  if((retval = poll(&pfd, 1, milliseconds)) == 1) {
    if (pfd.revents & POLLIN) retmask |= kReadable;
    if (pfd.revents & POLLOUT) retmask |= kWritable;
    if (pfd.revents & POLLERR) retmask |= kWritable;
    if (pfd.revents & POLLHUP) retmask |= kWritable;
    return retmask;
  } else {
    return retval;
  }
}


void WorkerThread::DealCmd(const std::string &cmd) {
  log_info("++DealCmd ============================");
  pthread_mutex_lock(&bufmu_);
  log_info("++Get buflock , I am %lu", pthread_self());
  int tmp = obuf_pos + obuf_len + cmd.size();
  log_info("++obuf_pos=%d obuf_len=%d tmp=%d", obuf_pos, obuf_len, tmp);
  while (obuf_pos + obuf_len + cmd.size() > 1024 * 16) {
    log_info("I wait");
    pthread_cond_wait(&bufwsignal_, &bufmu_);
    log_info("I weak");
  }
  log_info("++start writing cmd to obuf");
  memcpy(obuf + obuf_pos + obuf_len, cmd.data(), cmd.size());

  log_info("++end writing cmd to obuf");
  obuf_len += cmd.size();
  log_info("++current len of buf %d", obuf_len);
  
  
  pthread_cond_signal(&bufrsignal_);
  pthread_mutex_unlock(&bufmu_);
  log_info("++Release buflock , I am %lu", pthread_self());
  log_info("++DealCmd over ======================");
}


void *WorkerThread::PipeMode() {
  int done = 0;
  int flags;
  int eof = 0;

  int fd = cli_->fd();
  log_info("fd : %d pid:%lu", fd, pthread_self()); 
  
  if ((flags = fcntl(fd, F_GETFL)) == -1) {
    log_err("fcntl(F_GETFL): %s", strerror(errno));
  }
  flags |= O_NONBLOCK;
  if (fcntl(fd, F_SETFL, flags) == -1) {
    log_err("fcntl(F_SETFL,O_NONBLOCK): %s", strerror(errno));
  }

  while(!done) {
    int mask = kReadable;
    if(!eof || obuf_len != 0) mask |= kWritable;

    mask = Wait(fd, mask, 1000);
    
    if(mask & kReadable) {
      log_info("Readable");
      cli_->Recv(NULL);
    }
    
    if(mask & kWritable) {

      log_info("--Writable");
      int loop_nwritten = 0;
      // if no data in obuf, wait
        log_info("--start write to fd");
      // write all data in obuf to fd
      pthread_mutex_lock(&bufmu_);
      while(obuf_len == 0) {
        log_info("--no data in buf"); 
        log_info("task_queue size = %lu", task_queue_.size());
        pthread_cond_wait(&bufrsignal_, &bufmu_);
      }

      log_info("--Get buflock , I am %lu", pthread_self());
      while(obuf_len != 0) {

        log_info("--start write to fd");
        log_info("--obuf_pos=%d obuf_len=%d", obuf_pos, obuf_len);
        int nwritten = write(fd, obuf+obuf_pos, obuf_len);
        log_info("--write %d", nwritten);
        if (nwritten == -1) {
          if(errno != EAGAIN && errno != EINTR) {
            log_err("Error writing to the server : %s", strerror(errno));
            return NULL;
          } else {
            nwritten = 0;
          }
        }
        obuf_len -= nwritten;
        obuf_pos += nwritten;
        loop_nwritten += nwritten;
        
        pthread_cond_signal(&bufwsignal_);

        if(loop_nwritten > WRITE_LOOP_MAX_BYTES) {
          // log_info("no deta from pipe");
          break;
        } 
      }
      if (obuf_len == 0) {
        obuf_pos = 0;
      }

      pthread_mutex_unlock(&bufmu_);
      log_info("--Release buflock , I am %lu", pthread_self());
    }
  }
  log_info("over in consumer");
  return NULL;
}

      
      // // write all obuf to fd
      // while(1) {
        // log_info("%d", obuf_len);
        // if(obuf_len != 0) {
          // // pthread_mutex_lock(&bufmu_);
          // log_info("start writing obuf to socket_fd");
          // int nwritten = write(fd, obuf+obuf_pos, obuf_len);
          // if(nwritten == -1) {
            // if(errno != EAGAIN && errno != EINTR) {
              // log_err("Error writing to the server : %s", strerror(errno));
              // return NULL;
            // } else {
              // nwritten = 0;
            // }
          // }
          // log_info("1");
          // obuf_len -= nwritten;
          // obuf_pos += nwritten;
          // if(obuf_len == 0) {
            // obuf_pos = 0;
            // // after write all data
            // pthread_cond_signal(&bufwsignal_);
            // break;
          // }

          // // pthread_mutex_unlock(&bufmu_);
          // loop_nwritten += nwritten;
          // // if(obuf_len != 0) break;
        // }
        // if((obuf_len == 0 && eof) ||
           // loop_nwritten > WRITE_LOOP_MAX_BYTES) {
          // // log_info("no deta from pipe");
          // break;
        /* } */
        // load from pipe, if buffer is empty
        // if(obuf_len == 0 && !eof) {
        //   int nread = read(receive_fd, obuf, sizeof(obuf));
        //
        //   if(nread == 0) {
        //     //char echo[] =
        //         // " */\r\n*2\r\n$4\r\nECHO\r\n$20\r\n01234567890123456789\r\n";
        //     done = 1;
        //     eof = 1;
        //
        //   } else if (nread == -1) {
        //     log_err("Error reading from pipe: %s", strerror(errno));
        //   } else {
        //     obuf_len = nread;
        //     obuf_pos = 0;
        //   }
        // }

        // no data to read from pipe or write too much data
 

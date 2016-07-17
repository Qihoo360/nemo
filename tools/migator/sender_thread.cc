#include "sender_thread.h"

SenderThread::SenderThread(pink::RedisCli *cli) :
  cli_(cli),
  buf_len_(0),
  buf_pos_(0),
  buf_r_cond_(&buf_mutex_),
  buf_w_cond_(&buf_mutex_) {
}

SenderThread::~SenderThread() {
  should_exit_ = true;
  delete cli_;
}
  
int SenderThread::Wait(int fd, int mask, long long milliseconds) {
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


void SenderThread::LoadCmd(const std::string &cmd) {
  pink::MutexLock l(&buf_mutex_);
  // looped buf--------------------------------------------------- 
  size_t writepoint = buf_pos_ + buf_len_ + cmd.size();
  bool isloop = false;
  while (writepoint > kBufSize) {
    if (writepoint % kBufSize < buf_pos_) {
      isloop = true;
      break; 
    } else {
      std::cout << "full " << buf_len_ << " " << buf_pos_ << " "  << cmd.size() <<  std::endl;
      buf_w_cond_.Wait();
    }
    writepoint = buf_pos_ + buf_len_ + cmd.size();
  }
  
  if (isloop) {
    size_t left = kBufSize - buf_len_ - buf_pos_;
    const char *cmdbuf = cmd.data();
    memcpy(buf_ + buf_pos_ + buf_len_ , cmdbuf, left);
    memcpy(buf_, cmdbuf + left, cmd.size() - left);
  } else {
    memcpy(buf_ + buf_pos_ + buf_len_ , cmd.data(), cmd.size());
  }
  // looped buf--------------------------------------------------- 
  
  // while (buf_pos_ + buf_len_ + cmd.size() > kBufSize) {
    // std::cout << "full " << buf_len_ << " " << buf_pos_ << " "  << cmd.size() <<  std::endl;
    // buf_w_cond_.Wait();
  // }
  // memcpy(buf_ + buf_pos_ + buf_len_ , cmd.data(), cmd.size());

  // buf_len_ += cmd.size();
  // buf_r_cond_.Signal();

}

void *SenderThread::ThreadMain() {
  int fd = cli_->fd();
  int flags;
  if ((flags = fcntl(fd, F_GETFL)) == -1) {
    log_err("fcntl(F_GETFL): %s", strerror(errno));
  }
  flags |= O_NONBLOCK;
  if (fcntl(fd, F_SETFL, flags) == -1) {
    log_err("fcntl(F_SETFL,O_NONBLOCK): %s", strerror(errno));
  }

  while(!should_exit_) {
    int mask = kReadable;
    // if(!eof || buf_len_ != 0) mask |= kWritable;
    if( buf_len_ != 0) mask |= kWritable;
    mask = Wait(fd, mask, 1000);
    if(mask & kReadable) {
      cli_->Recv(NULL);
    }
    if(mask & kWritable) {
      size_t loop_nwritten = 0;
      while (1) {
        size_t len; 
        {
          pink::MutexLock l(&buf_mutex_);
          while (buf_len_ == 0) {
            std::cout << "empty" << std::endl;
            buf_r_cond_.Wait();
          } 
          len = buf_len_;
        } 

        // looped buf 
        int nwritten = 0;
        if (buf_pos_ + len > kBufSize) {
          size_t left = kBufSize - buf_pos_;
          nwritten = write(fd, buf_ + buf_pos_, left);
          nwritten += write(fd, buf_, len - left);
        } else {
          nwritten = write(fd ,buf_ + buf_pos_, len); 
        }

        if (nwritten == -1) {
          if (errno != EAGAIN && errno != EINTR) {
            log_err("Error writting to the server : %s", strerror(errno));
            return NULL;
          } else {
            nwritten = 0;
          }  
        }
        {
          pink::MutexLock l(&buf_mutex_);
          buf_len_ -= nwritten;
          buf_pos_ = (buf_pos_ + nwritten) % kBufSize;
          loop_nwritten += nwritten;
          buf_w_cond_.Signal();
        } 
  

        // int nwritten = write(fd, buf_ + buf_pos_, len);
        // if (nwritten == -1) {
          // if (errno != EAGAIN && errno != EINTR) {
            // log_err("Error writting to the server : %s", strerror(errno));
            // return NULL;
          // } else {
            // nwritten = 0;
          // }  
        // }
        // {
          // pink::MutexLock l(&buf_mutex_);
          // buf_len_ -= nwritten;
          // buf_pos_ += nwritten;
          // loop_nwritten += nwritten;
          // buf_w_cond_.Signal();
          // if (buf_len_ == 0) {
            // buf_pos_ = 0;
          // }
        // }

        if (loop_nwritten > kWirteLoopMaxBYTES ) {
          break;
        } 
      }
    } // end of writable    
  }   // end of while
  return NULL;
}



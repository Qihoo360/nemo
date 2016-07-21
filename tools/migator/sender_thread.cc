#include "sender_thread.h"

SenderThread::SenderThread(pink::RedisCli *cli) :
  cli_(cli),
  buf_len_(0),
  buf_pos_(0),
  buf_r_cond_(&buf_mutex_),
  buf_w_cond_(&buf_mutex_),
  num_(0){
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
  while (buf_pos_ + buf_len_ + cmd.size() > kBufSize) {
    buf_w_cond_.Wait();
  }
  memcpy(buf_ + buf_pos_ + buf_len_ , cmd.data(), cmd.size());
  buf_len_ += cmd.size();
  PlusNum();
  buf_r_cond_.Signal();
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

  while (!should_exit_ || buf_len_ != 0 || num() > 0) {
    int mask = kReadable;
    if( buf_len_ != 0) mask |= kWritable;
    mask = Wait(fd, mask, 1000);
    if(mask & kReadable) {
      if (rbuf_pos_ > 0) {
        if (rbuf_offset_ > 0) {
          memmove(rbuf_, rbuf_ + rbuf_pos_, rbuf_offset_);
        }
        rbuf_pos_ = 0;
      }

      // read from socket
      ssize_t nread;
      do {
        nread = read(fd, rbuf_ + rbuf_offset_, rbuf_size_ - rbuf_offset_);
        // std::cout << "nread=" << nread << "\n";

        if (nread == -1) {
          if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
            continue;
          } else {
            log_err("Error reading from the server : %s", strerror(errno));
            return NULL;
          }
        }
        rbuf_offset_ = 0;
        // rbuf_offset_ += nread;
      } while (nread > 0);
      // std::cout << "stop read" << std::endl;
    }
    if(mask & kWritable) {
      size_t loop_nwritten = 0;
      while (1) {
        size_t len; 
        {
          pink::MutexLock l(&buf_mutex_);
          if (buf_len_ == 0) { 
            if (loop_nwritten > kWirteLoopMaxBYTES)
            {
              break;
            }
            buf_r_cond_.Wait();
          } 
          if (buf_len_ > 1024 * 16) {
            len = 1024 * 16;
          } else {
            len = buf_len_;
          }
        } 

        int nwritten = write(fd, buf_ + buf_pos_, len);
        if (nwritten == -1) { 
          if (errno != EAGAIN && errno != EINTR) {
            std::cout << "buf_pos=" << buf_pos_ << " len=" << len << std::endl; 
            log_err("Error writting to the server : %s", strerror(errno));
            return NULL;
          } else {
            nwritten = 0;
          }  
        }
        {
          pink::MutexLock l(&buf_mutex_);
          buf_len_ -= nwritten;
          buf_pos_ += nwritten;
          loop_nwritten += nwritten;
          buf_w_cond_.Signal();

          if (buf_len_ == 0) {
            buf_pos_ = 0;
          }
          
          if (nwritten < len) {
            // std::cout << "nwritten=" << nwritten << " len=" << len <<   " quit writing\n";
            break;
          }
       }
       if (loop_nwritten > kWirteLoopMaxBYTES ) {
          break;
       }  
      }
    } // end of writable    
  }   // end of while
  return NULL;
}



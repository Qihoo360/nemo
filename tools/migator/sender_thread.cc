#include "sender_thread.h"

enum REDIS_STATUS {
  REDIS_ETIMEOUT = -2,
  REDIS_ERR = -1,
  REDIS_OK = 0,
  REDIS_HALF,
  REDIS_REPLY_STRING,
  REDIS_REPLY_ARRAY,
  REDIS_REPLY_INTEGER,
  REDIS_REPLY_NIL,
  REDIS_REPLY_STATUS,
  REDIS_REPLY_ERROR
};


SenderThread::SenderThread(pink::RedisCli *cli) :
  cli_(cli),
  buf_len_(0),
  buf_pos_(0),
  buf_r_cond_(&buf_mutex_),
  buf_w_cond_(&buf_mutex_)
  {
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

  if ((retval = poll(&pfd, 1, milliseconds)) == 1) {
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

  while (!should_exit_ || buf_len_ != 0) {
    int mask = kReadable;
    if ( buf_len_ != 0) mask |= kWritable;
    mask = Wait(fd, mask, 1000);
    if (mask & kReadable) {
      if (rbuf_pos_ > 0) {
        if (rbuf_offset_ > 0) {
          memmove(rbuf_, rbuf_ + rbuf_pos_, rbuf_offset_);
        }
        rbuf_pos_ = 0;
      }
      // read from socket
      ssize_t nread;
      do {
        nread = read(fd, rbuf_ + rbuf_pos_ + rbuf_offset_,
                     rbuf_size_ - rbuf_pos_ - rbuf_offset_);
        if (nread == -1 && errno != EINTR && errno != EAGAIN) {
          log_err("Error reading from the server : %s", strerror(errno));
        }
        if (nread > 0) {
          rbuf_offset_ += nread;

          int status = TryRead();
          if (status == REDIS_HALF) {
            // std::cout << "REDIS_HALF\n";
            continue;
          } else if (status == REDIS_ERR) {
            log_err("Bad data from server");
          }
        }
      } while (nread > 0);
    }
    if (mask & kWritable) {
      size_t loop_nwritten = 0;
      while (1) {
        size_t len; 
        {
          pink::MutexLock l(&buf_mutex_);
          if (buf_len_ == 0) { 
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
          if ((size_t)nwritten < len) {
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

int SenderThread::TryRead() {
  if (rbuf_offset_ == 0) {
    return REDIS_HALF;
  }

  char *p;
  if ((p = ReadBytes(1)) == NULL) {
    return REDIS_HALF;
  }

  int type;
  switch (*p) {
    case '-':
      type = REDIS_REPLY_ERROR;
      break;
    case '+':
      type = REDIS_REPLY_STATUS;
      break;
    case ':':
      type = REDIS_REPLY_INTEGER;
      break;
    case '$':
      type = REDIS_REPLY_STRING;
      break;
    case '*':
      type = REDIS_REPLY_ARRAY;
      break;
    default:
      return REDIS_ERR;
  }

  int len;
  // when not a complete, p == NULL 
  if ((p = ReadLine(&len)) == NULL) {
    rbuf_offset_ -= 1;
    rbuf_pos_ -= 1;
    return REDIS_HALF;
  }
  
  if (type == REDIS_REPLY_ERROR) {
    err_++;
    std::cout << std::string(p, len) << std::endl;
  } else if (type == REDIS_OK) {
    elements_++;
  }

  return REDIS_OK;
}

char *SenderThread::ReadLine(int *_len) {
  char *p, *s;
  int len;

  p = rbuf_ + rbuf_pos_;
  s = seekNewline(p, rbuf_offset_);
  if (s != NULL) {
    len = s - (rbuf_ + rbuf_pos_); 
    rbuf_pos_ += len + 2; /* skip \r\n */
    rbuf_offset_ -= len + 2;
    if (_len) *_len = len;
    return p;
  }
  return NULL;
}
/* Find pointer to \r\n. */
char *SenderThread::seekNewline(char *s, size_t len) {
  int pos = 0;
  int _len = len - 1;

  /* Position should be < len-1 because the character at "pos" should be
   * followed by a \n. Note that strchr cannot be used because it doesn't
   * allow to search a limited length and the buffer that is being searched
   * might not have a trailing NULL character. */
  while (pos < _len) {
    while (pos < _len && s[pos] != '\r') pos++;
    if (s[pos] != '\r') {
      /* Not found. */
      return NULL;
    } else {
      if (s[pos+1] == '\n') {
        /* Found. */
        return s+pos;
      } else {
        /* Continue searching. */
        pos++;
      }
    }
  }
  return NULL;
}

char* SenderThread::ReadBytes(unsigned int bytes) {
  char *p = NULL;
  if ((unsigned int)rbuf_offset_ >= bytes) {
    p = rbuf_ + rbuf_pos_;
    rbuf_pos_ += bytes;
    rbuf_offset_ -= bytes;
  }
  return p;
}


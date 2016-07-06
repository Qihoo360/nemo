#include <iostream>
#include <string>
#include <sstream>
#include <sys/poll.h>
#include "redis_cli.h"
#include "nemo.h"
#include "nemo_const.h"

using namespace std;
using namespace pink;
using namespace nemo;

const int64_t kTestPoint = 1;
const int64_t kTestNum = 100; 

const int WRITE_LOOP_MAX_BYTES = 128 * 1024;
// const int64_t kTestNum = ULLONG_MAX; 
int receive_fd;
int send_fd;
int socket_fd;
int len = 1024 * 16;
char ibuf[1024*16];
char obuf[1024*16];
int obuf_len = 0;
int obuf_pos = 0;
char cmdbuf[1024*16];
pink::RedisCli *cli;


void MigrateKDB(nemo::Nemo *db, pink::RedisCli *cli);
void MigrateHDB(nemo::Nemo *db, pink::RedisCli *cli);
void MigrateZDB(nemo::Nemo *db, pink::RedisCli *cli);
void MigrateSDB(nemo::Nemo *db, pink::RedisCli *cli);
void MigrateLDB(nemo::Nemo *db, pink::RedisCli *cli);

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

  int TotalTime() {
    return micorsec / 1000000;
  }
 private:
  struct timeval tv_start;
  struct timeval tv_end;
  float micorsec;
};


FunctionTimer dealcmd_timer;

class Timer {
 public:

  ~Timer() {
    // delete p;
  }

  void Start(const string &msg) {
    time(&start);
    // p = gmtime(&start);
    // p = localtime(&start);

    // cout << "[" <<  p->tm_hour << ":" << p->tm_min << ":" << p->tm_sec << "]"; 
    // cout << msg << endl;
  } 

  void End(const string &msg, const string &domsg) {
    time(&end);
    // p = gmtime(&end);
    // p = localtime(&start);
    // string msg = "Runing time : ";
    // log_info("Running time:");

    // cout << "[" <<  p->tm_hour << ":" << p->tm_min << ":" << p->tm_sec << "]"; 
    // cout << msg;
    // cout << domsg << " Running time: " << difftime(end, start) << " s" << endl; 
  } 

  void Click(const string &msg) {
    time(&end);
    // p = localtime(&end);
    // cout << "[" <<  p->tm_hour << ":" << p->tm_min << ":" << p->tm_sec << "]";
    // cout << msg << endl;
  }

 private:
  time_t start;
  time_t end;
  struct tm *p;
};

void Usage() {
  cout << "Usage: " << endl;
  cout << "./p2r db_path ip port" << endl;
}

string GetKey(const rocksdb::Iterator *it) {
  return it->key().ToString().substr(1);
}

// pipe mode. Write raw protocol to a instream 
pink::Status DealCmd(std::string &cmd, pink::RedisCli *cli) {
  dealcmd_timer.Start();

  memcpy(cmdbuf, cmd.c_str(), cmd.size());

  write(send_fd, cmdbuf, strlen(cmdbuf));

  pink::Status s; 
  dealcmd_timer.Click();
  return s;
}


const int kReadable = 1;
const int kWritable = 2;
int Wait(int fd, int mask, long long milliseconds) {
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

void *PipeMode(void*) {
  int done = 0;
  int flags;
  int eof = 0;
  if ((flags = fcntl(socket_fd, F_GETFL)) == -1) {
    log_err("fcntl(F_GETFL): %s", strerror(errno));
  }
  log_info("socket_fd %d", flags);

  flags |= O_NONBLOCK;
  if (fcntl(socket_fd, F_SETFL, flags) == -1) {
    log_err("fcntl(F_SETFL,O_NONBLOCK): %s", strerror(errno));
  }

  if ((flags = fcntl(0, F_GETFL)) == -1) {
    log_err("fcntl(F_GETFL): %s", strerror(errno));
  }
  log_info("stdin %d", flags);
  // flags |= O_NONBLOCK;
  

  if (fcntl(receive_fd, F_SETFL, flags) == -1) {
    log_err("fcntl(F_SETFL,O_NONBLOCK): %s", strerror(errno));
  }

  log_info("receive_fd %d", flags);
  int fd = socket_fd;

  while(!done) {
    int mask = kReadable;
    if(!eof || obuf_len != 0) mask |= kWritable;
   
    mask = Wait(fd, mask, 1000);

    log_info("the mask %d", mask);
    if(mask & kReadable) {
      cli->Recv(NULL);
    } 

    if(mask & kWritable) {
      int loop_nwritten = 0;

      while(1) {
        if(obuf_len != 0) {
          int nwritten = write(fd, obuf+obuf_pos, obuf_len);
          log_info("has written %d", nwritten);

          if(nwritten == -1) {
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
          if(obuf_len != 0) break;
        }
        // load from pipe, if buffer is empty
        if(obuf_len == 0 && !eof) {
          log_info("Read from pipe");
          int nread = read(receive_fd, obuf, sizeof(obuf));
          
          log_info("read from pip =========================%d", nread);

          if(nread == 0) {
            log_info("over==================================================");
           /*  char echo[] =  */
                // " */\r\n*2\r\n$4\r\nECHO\r\n$20\r\n01234567890123456789\r\n";

            done = 1;
            eof = 1;

          } else if (nread == -1) {
            log_err("Error reading from pipe: %s", strerror(errno));
          } else {
            obuf_len = nread;
            obuf_pos = 0;
          }
        }

        // no data to read from pipe or write too much data
        if((obuf_len == 0 && eof) || 
           loop_nwritten > WRITE_LOOP_MAX_BYTES) {
          log_info("no deta from pipe");
          break;
        }
      }
    }
  }

  log_info("over in consumer");
  

  return NULL;
}
 
int main(int argc, char **argv) 
{

  // string db_path = "/home/yinshucheng/pika/output/db/";
  string db_path = "/home/yinshucheng/db2/";
  string ip = "127.0.0.1"; 
  int port = 6379;

  // if (argc != 4) {  
    // Usage();
    // log_err("");
  // } 
  // string db_path(argv[1]);  
  // string ip(argv[2]);
  // int port = atoi(argv[3]);

  if(argc == 4) {
    db_path = string(argv[1]); 
    ip = string(argv[2]);
    port = atoi(argv[3]);
  }
   

  cout << db_path << endl;
  nemo::Options option;
  option.write_buffer_size = 256 * 1024 * 1024;
  option.target_file_size_base = 20 * 1024 * 1024;
  
  log_info("Prepare DB..");
  nemo::Nemo *n = new nemo::Nemo(db_path ,option);
  
  
  if (n == NULL)
  {
    // cout << "db is NULL" << endl;
  }

  nemo::Status nemo_s;
  pink::Status pink_s;
  // string cmd;

  // init redis-cli
  cli = new RedisCli(); 
  cli->set_connect_timeout(3000);
  pink_s = cli->Connect(ip, port);
  if(!pink_s.ok()) {
    cout << pink_s.ToString() << endl;
    cout << "ip : " << ip << endl;
    cout << "port : " << port << endl;
    return -1;
  }
  socket_fd = cli->fd();

  // init pipe
  int fds[2];
  if (pipe(fds)) {
    log_err("Can't create pipe");
    return -1;
  }
  receive_fd = fds[0];
  send_fd = fds[1];


  log_info("socket : %d receive_fd %d send_fd %d", socket_fd, receive_fd, send_fd);
  

  pthread_t consumer;
  // pthread_t producer;

  pthread_create(&consumer, NULL, PipeMode, NULL);

 

  Timer timer;
  timer.Start("Program start");

  MigrateKDB(n, cli);
  // MigrateHDB(n, cli); 
  // MigrateSDB(n, cli); 
  // MigrateZDB(n, cli);
  // MigrateLDB(n, cli);

 /*  close(receive_fd); */
  // close(send_fd);


  log_info("scan over");
  pthread_join(consumer, NULL);
  timer.End("Program end", "");

   // statistic info
  cout << "dealcmd function running :" << dealcmd_timer.TotalTime() << endl;

  delete cli;
  delete n;
 
  return 0;
}


// Test HiRedisCli pipe mode
// pink::Status DealCmd(string &cmd, RedisCli *cli) {
  // dealcmd_timer.Start();
  // pink::Status s;
  // cout << cmd;
  // dealcmd_timer.Click();
  // return s;
// }

// non-pipe mode
// pink::Status DealCmd(string &cmd, RedisCli *cli) {
  // dealcmd_timer.Start();
  // pink::Status s;
  // s = cli->Send((void *)&cmd);
  // if (!s.ok()) {
    // cout << s.ToString() << endl;
  // }
  // s = cli->Recv(NULL);
   // if (!s.ok()) {
     // cout << s.ToString() << endl;
  // }
  // dealcmd_timer.Click();
  // return s;
// }

   
void MigrateKDB(nemo::Nemo *n,RedisCli *cli) { 
  Timer timer;
  timer.Start("KvDB migration start");
  int64_t num = 0;
 
  KIterator *iter = n->KScan("", "", -1);

  for (; iter->Valid(); iter->Next()) {
    pink::RedisCmdArgsType argv;
    string cmd;
 
    argv.push_back("SET");
    argv.push_back(iter->key());
    argv.push_back(iter->value());
    pink::RedisCli::SerializeCommand(argv, &cmd);

    num++;
    if(num % kTestPoint == 0) {
      stringstream ss;
      ss << "Has migrated KvDB " << num << " record";
      timer.Click(ss.str());
      if(num == kTestNum) {
        break; 
        // return;
      }
    }

    DealCmd(cmd, cli);
  }

  delete iter;
  stringstream ss;
  ss << "KvDB ";
  ss << num;
  ss << " records";
  timer.End("KvDB migration end", ss.str());
}

 
void MigrateHDB(nemo::Nemo *n,RedisCli *cli) { 
  Timer timer;
  timer.Start("HashDB migration start");
  int64_t num = 0;
 
  rocksdb::Iterator *keyIter = n->KeyIterator(nemo::DataType::kHSize);
  for (; keyIter->Valid(); keyIter->Next()) {
    string key = GetKey(keyIter);
    if (key.length() == 0) break;
    HIterator *iter = n->HScan(key, "", "", -1, false);
    bool isover = false;
  
    for (; iter->Valid(); iter->Next()) {
      pink::RedisCmdArgsType argv;
      string cmd;
    
      argv.push_back("HSET");
      argv.push_back(iter->key());
      argv.push_back(iter->field());
      argv.push_back(iter->value());
      pink::RedisCli::SerializeCommand(argv, &cmd);

      DealCmd(cmd, cli);

      num++;
      if(num % kTestPoint == 0) {
        stringstream ss;
        ss << "Has migrated HashDB " << num << " record";
        timer.Click(ss.str());
        if(num == kTestNum) {
          isover = true;
          break;
        }
      }
 
    }
    
    delete iter;
    if(isover) {
      break;
    }
  }

  delete keyIter;
  stringstream ss;
  ss << "HashDB ";
  ss << num;
  ss << " records";
  timer.End("HashDB migration end", ss.str());
}

void MigrateSDB(nemo::Nemo *n,RedisCli *cli) { 
  Timer timer;
  timer.Start("SetDB migration start");
  int64_t num = 0; 
  rocksdb::Iterator *keyIter = n->KeyIterator(nemo::DataType::kSSize);
  for (; keyIter->Valid(); keyIter->Next()) {
    string key = GetKey(keyIter);
    if (key.length() == 0) break;
    SIterator *iter = n->SScan(key, -1, false);
    bool isover = false;
  
    for (; iter->Valid(); iter->Next()) {
      pink::RedisCmdArgsType argv;
      string cmd;
    
      argv.push_back("SADD");
      argv.push_back(iter->key());
      argv.push_back(iter->member());
      pink::RedisCli::SerializeCommand(argv, &cmd);

      DealCmd(cmd, cli);
    
      num++;
      if(num % kTestPoint == 0) {
        stringstream ss;
        ss << "Has migrated SetDB " << num << " record";
        timer.Click(ss.str());
        if(num == kTestNum) {
          isover = true;
          break;
        }
      }
    }
    delete iter;

    if(isover) break;
  }
  delete keyIter;

  stringstream ss;
  ss << "SetDB ";
  ss << num;
  ss << " records";
  timer.End("SetDB migration end", ss.str());

}


void MigrateZDB(nemo::Nemo *n,RedisCli *cli) { 
  Timer timer;
  timer.Start("ZSetDB migration start");
  int64_t num = 0; 

  rocksdb::Iterator *keyIter = n->KeyIterator(nemo::DataType::kZSize);
  for (; keyIter->Valid(); keyIter->Next()) {
    string key = GetKey(keyIter);
    if (key.length() == 0) break;
    ZIterator *iter = n->ZScan(key, ZSET_SCORE_MIN, ZSET_SCORE_MAX, -1, false);
    bool isover = false;

    for (; iter->Valid(); iter->Next()) {
      pink::RedisCmdArgsType argv;
      string cmd;
      
      argv.push_back("ZADD");
      argv.push_back(iter->key());

      double score = iter->score();
      string str_score;
      stringstream ss;
      ss << score;
      ss >> str_score;
      argv.push_back(str_score);
      
      argv.push_back(iter->member());
      pink::RedisCli::SerializeCommand(argv, &cmd);

      DealCmd(cmd, cli);
    
      num++;
      if(num % kTestPoint == 0) {
        stringstream ss;
        ss << "Has migrated ZSetDB " << num << " record";
        timer.Click(ss.str());
        if(num == kTestNum) {
          isover = true;
          break;
        }

      }
 
    }
    delete iter;
    if(isover) {
      break;
    }
  }
  delete keyIter;
  
  stringstream ss;
  ss << "ZSetDB ";
  ss << num;
  ss << " records";
  timer.End("ZSetDB migration end", ss.str());
}

 
void MigrateLDB(nemo::Nemo *n,RedisCli *cli) { 
  Timer timer;
  timer.Start("ListDB migration start");
  int64_t num = 0; 

  rocksdb::Iterator *keyIter = n->KeyIterator(nemo::DataType::kLMeta);
  for (; keyIter->Valid(); keyIter->Next()) {
    string key = GetKey(keyIter);
    if (key.length() == 0) break;

    bool isover = false;
    // cout << "LIST KEY :" << key << endl;
    std::vector<IV> ivs;
  
    int64_t begin = 0;
    int64_t offset = 512;
    int64_t end = begin + offset;
    // cout << "begin = " << begin << " end=" << end << endl;
    n->LRange(key, begin, end, ivs);
    // n->LRange(key, 500, 550, ivs);
    std::vector<IV>::const_iterator it;
   
    while(!ivs.empty()) {
      pink::RedisCmdArgsType argv;
      string cmd;
    
      argv.push_back("RPUSH");
      argv.push_back(key);
    
      // cout << "ivs size = " << ivs.size() << endl;
      for (it = ivs.begin(); it != ivs.end(); ++it) {
        // log_info("Test LRange index =  %ld, val = %s", (*(it)).index, (*(it)).val.c_str());
        argv.push_back(it->val);
      
        num++;
      }
       
      pink::RedisCli::SerializeCommand(argv, &cmd);
      DealCmd(cmd, cli);
     
      if(num % kTestPoint == 0) {
        stringstream ss;
        ss << "Has migrated ListDB " << num << " record";
        timer.Click(ss.str());
        if(num == kTestNum) {
          isover = true;
          break;
        }
      }
      
      begin += offset;
      end = begin + offset;
      ivs.clear();
      n->LRange(key, begin, end, ivs);
    } 

    if(isover) break;
  }  
  
  delete keyIter;
  stringstream ss;
  ss << "ListDB ";
  ss << num;
  ss << " records";
  timer.End("ListDB migration end", ss.str());

}



#include <iostream>
#include <string>
#include <sstream>
#include "redis_cli.h"
#include "nemo.h"
#include "nemo_const.h"

using namespace std;
using namespace pink;
using namespace nemo;

const int64_t kTestPoint = 100;
const int64_t kTestNum = 10000; 
// const int64_t kTestNum = ULLONG_MAX; 

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


pink::Status DealCmd(string &cmd, RedisCli *cli) {
  dealcmd_timer.Start();
  pink::Status s;
  cout << cmd;
  dealcmd_timer.Click();
  return s;
}

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

int main(int argc, char **argv) 
{

  // string db_path = "/home/yinshucheng/pika/output/db/";
  string db_path = "/home/yinshucheng/db/";
  string ip = "127.0.0.1"; 
  int port = 6379;

  // if (argc != 4) {  
    // Usage();
    // log_err("");
  /* } */
/*   string db_path(argv[1]);  */
  // string ip(argv[2]);
  // int port = atoi(argv[3]);

  if(argc == 4) {
    db_path = string(argv[1]); 
    ip = string(argv[2]);
    port = atoi(argv[3]);
  }
   

  // cout << db_path << endl;
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
  RedisCli *cli = new RedisCli(); 
  cli->set_connect_timeout(3000);
  pink_s = cli->Connect(ip, port);
  if(!pink_s.ok()) {
    cout << pink_s.ToString() << endl;
    cout << "ip : " << ip << endl;
    cout << "port : " << port << endl;
    return -1;
  }


  // int cli_num = 5;
  // RedisCli clis[cli_num];
  // for(int i = 0; i < cli_num; i++) {
    // clis[i] = new RedisCli();
    // clis[i]->set_connect_timeout(3000);
    // clis[i]->Connect(ip, port);
  // }

  Timer timer;
  timer.Start("Program start");

  // MigrateKDB(n, cli);
  MigrateHDB(n, cli); 
  // MigrateSDB(n, cli); 
  // MigrateZDB(n, cli);
  // MigrateLDB(n, cli);

  timer.End("Program end", "");


  cout << "dealcmd function running :" << dealcmd_timer.TotalTime() << endl;

  delete cli;
  delete n;
 
  return 0;
}


#include <iostream>

#include <vector>
#include <string>
#include <ctime>
#include <stdint.h>
#include <limits>
#include <thread>
#include <algorithm>
#include <mutex>
#include <cstdlib>
#include <cctype>
#include <fstream>

#include "nemo.h"
#include "xdebug.h"

#include "time_keeper.h"
#include "random_string.h"
#include "config.h"
using namespace nemo;


void GetKVs(std::vector<nemo::KV> &kv, const std::vector<std::string> &key, const std::vector<std::string> &value) { 
  std::vector<std::string>::const_iterator it_k = key.begin();
  std::vector<std::string>::const_iterator it_v = value.begin();

  while (it_k != key.end() && it_v != value.end()) {
    nemo::KV temp;
    temp.key = *it_k++;
    temp.val = *it_v++;
    kv.push_back(temp);
  }
}

int main(int argc, char* argv[])
{

  std::vector<int> num_of_keys({10000, 100000});
  std::vector<int> key_size({20});
  std::vector<int> value_size({10, 1000, 1000000});
  std::vector<int> clients({10, 20, 40}); 
  std::vector<int> requests({1000000});
  std::vector<int> opsv({10, 100});
  std::vector<std::string> op({ "set", "get", "mset", "mget",
                                "hset", "hget",
                                "lset", "lpush", "lpop",
                                "zadd", "zcard", "zrem",
                                "sadd", "srem"
                                });
  std::map<std::string,int> cmd_map;    
  for(int j =0; j < cmdLen; j++) {
    cmd_map.insert(std::pair < std::string,int>(cmd[j], j));
  }

  //start..... 
  for (int h = 0; h < op.size(); h++) {                //operation
    for (int i = 0; i < clients.size(); i++) {        //clients
      for (int j = 0; j < value_size.size(); j++) {    //valueSize
        for (int k = 0; k < key_size.size(); k++) {    //keySize
          int flag=1;
          if(op[h].compare("mset")==0||op[h].compare("mget")==0)
            flag=0;
          for (int m = 0; m < opsv.size(); m++) {     //specific parameter for some operations such as mset,mget, etc.
            for (int n = 0; n < requests.size(); n++) { //num of requests
              for (int p = 0; p < num_of_keys.size(); p++) {
                //shell command
                std::system("cp -r ./tmp ./tmp2");

                //parameter configure

                Config conf;   
                conf.folder = "./record/";
                conf.op = cmd_map.find(op[h])->second;
                conf.num_of_clients = clients[i];
                conf.value_size = value_size[j];
                conf.key_size = key_size[k];
                conf.m = opsv[m];
                conf.num_of_requests = requests[n];
                conf.num_of_keys = num_of_keys[p];


                if (conf.value_size == 1000000) conf.num_of_keys = 10000;
                conf.finished_requests = 0;
                conf.latency = std::vector<int64_t>(conf.num_of_requests + 3 * conf.num_of_clients, 0);
                RandomString  rs; 
                conf.key.reserve(conf.num_of_keys);
                conf.value.reserve(conf.num_of_keys);
                conf.field.reserve(conf.num_of_keys);
                conf.kv.reserve(conf.num_of_keys);
                conf.ttl.reserve(conf.num_of_keys);

                //random strings
                rs.GetRandomKeyValues(conf.key, conf.value, conf.key_size, conf.value_size, conf.num_of_keys);
                GetKVs(conf.kv,conf.key,conf.value);
                rs.GetRandomFields(conf.field,conf.key_size,conf.num_of_keys);
                rs.GetRandomInts(conf.ttl,conf.num_of_keys);
                //over!
                std::cout<<"================================\n";
                std::cout<<"random string over\n"<<std::endl;

                //test start
                std::cout<<"operation= "<<op[h]<<std::endl;
                std::cout<<"num Of Clients= "<<clients[i]<<std::endl;
                std::cout<<"valueSize= "<<value_size[j]<<std::endl;
                std::cout<<"playLoad= "<<key_size[k]<<std::endl;
                std::cout<<"m value= "<<opsv[m]<<std::endl;
                std::cout<<"numOfRequests= "<<requests[n]<<std::endl;
                std::cout<<"numOfKeys= "<<num_of_keys[p]<<std::endl; 
                std::cout<<"...........\n";

                nemo::Options options;
                options.target_file_size_base = 20 * 1024 * 1024; 
                options.write_buffer_size = 256 * 1024 * 1024;
                nemo::Nemo *np = new nemo::Nemo("./tmp", options);
                std::string file_name = op[h]; 
                TestOPs(np, &conf);
                ShowReports(conf, conf.folder+file_name);

                //parameter reset
                Reset(conf);

                delete np;
                //shell command
                std::system("rm -rf ./tmp ");
                std::system("cp -r ./tmp2 ./tmp");
                std::system("rm -rf ./tmp2 ");
                std::system("sudo sync; echo 3 > /proc/sys/vm/drop_caches");

              }
            }
          }
        }
      }
    }
  }
  exit(0);
}





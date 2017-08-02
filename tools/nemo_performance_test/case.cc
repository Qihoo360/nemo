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
using std::vector;




int main(int argc, char* argv[])
{
  int playLoad=3,dataSize=3;

  std::vector<int> numOfKeys({10000,100000});
  std::vector<int> keySize({20});
  std::vector<int> valueSize({10,1000,1000000});
  std::vector<int> clients({10,20,40}); 
  std::vector<int> requests({1000000});
  std::vector<int> opsv({10,100});
  std::vector<std::string> op({"set","get","mset","mget","hset", "hget","lset","lpush","lpop",
      "zadd","zcard","zrem","sadd","srem"});
  std::map<std::string,int> cmdMap;    
  for(int j =0; j<cmdLen;j++){
    cmdMap.insert(std::pair<std::string,int>(cmd[j],j));
  }

  //start..... 
  for(int h=0;h<op.size();h++){                //operation
    for(int i=0;i<clients.size();i++){        //clients
      for(int j=0;j<valueSize.size();j++){    //valueSize
        for(int k=0;k<keySize.size();k++){    //keySize
          for(int m=0;m<opsv.size();m++){     //specific parameter for some operations such as mset,mget, etc.
            for(int n=0;n<requests.size();n++){ //num of requests
              for(int p=0;p<numOfKeys.size();p++){
                //shell command
                std::system("cp -r ./tmp ./tmp2");

                //parameter configure

                config conf;   
                conf.folder="./record/";
                conf.op=cmdMap.find(op[h])->second;
                conf.numOfClients=clients[i];
                conf.valueSize=valueSize[j];
                conf.playLoad=keySize[k];
                conf.m=opsv[m];
                conf.numOfRequests=requests[n];
                conf.numOfKeys=numOfKeys[p];


                if(conf.valueSize==1000000) conf.numOfKeys=10000;
                conf.finishedRequests=0;
                conf.latency=vector<int64_t>(conf.numOfRequests+3*conf.numOfClients,0);
                random_string  rs; 
                conf.key.reserve(conf.numOfKeys);
                conf.value.reserve(conf.numOfKeys);
                conf.field.reserve(conf.numOfKeys);
                conf.kv.reserve(conf.numOfKeys);
                conf.ttl.reserve(conf.numOfKeys);

                //random strings
                rs.getRandomKeyValues(conf.key,conf.value,conf.playLoad,conf.valueSize,conf.numOfKeys);
                rs.getRandomKVs(conf.kv,conf.playLoad,conf.valueSize,conf.numOfKeys); 
                rs.getRandomFields(conf.field,conf.playLoad,conf.numOfKeys);
                rs.getRandomInts(conf.ttl,conf.numOfKeys);
                //over!
                std::cout<<"================================\n";
                std::cout<<"random string over\n"<<std::endl;

                //test start
                std::cout<<"operation= "<<op[h]<<std::endl;
                std::cout<<"num Of Clients= "<<clients[i]<<std::endl;
                std::cout<<"valueSize= "<<valueSize[j]<<std::endl;
                std::cout<<"playLoad= "<<keySize[k]<<std::endl;
                std::cout<<"m value= "<<opsv[m]<<std::endl;
                std::cout<<"numOfRequests= "<<requests[n]<<std::endl;
                std::cout<<"numOfKeys= "<<numOfKeys[p]<<std::endl; 
                std::cout<<"...........\n";

                nemo::Options options;
                options.target_file_size_base=20*1024*1024; 
                options.write_buffer_size=256*1024*1024;
                nemo::Nemo *np=new nemo::Nemo("./tmp",options);
                std::string fileName=op[h]; 
                test_ops(np,&conf);
                showReports(conf,conf.folder+fileName);

                //parameter reset
                reset(conf);


                delete np;
                //shell command
                std::system("rm -rf ./tmp ");
                std::system("cp -r ./tmp2 ./tmp");
                std::system("rm -rf ./tmp2 ");
                std::system("sync; echo 3 > /proc/sys/vm/drop_caches");

              }
            }
          }
        }
      }
    }
  }
  exit(0);
}





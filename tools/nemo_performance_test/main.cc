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

#include "nemo.h"
#include "xdebug.h"

#include "time_keeper.h"
#include "random_string.h"
#include "config.h"
using namespace nemo;

void Usage() {
  std::cout<<"Usage: nemo-benchmark: -t <set,setttl,get...> [-c <clients>] [-r <keyspacelen>] [-n <requests>]\n"<<
    "      -c <clients>  number of clients(default 1)\n"<<
    "      -r <keyspacelen> number of random keys&values (default 1000)\n"<<
    "      -n <requests> total number of requests (default 100000) \n"<<
    "      -d <size> size of SET/GET key in bytes (default 3)\n"<<
    "      -s <size> size of SET/GET value in bytes (default 3)\n"<<
    "      -t <cmd> commend that you want to test\n";
  exit(0);
}

void ParseOption(int argc,char *argv[],Config &conf) {
  if (argc%2 == 0) {
    Usage();
  }

  std::map<std::string,int> cmdMap;    
  for (int j = 0; j < cmdLen; j++) {
    cmdMap.insert(std::pair<std::string,int>(cmd[j],j));
  }

  for (int i = 1; i < argc; i += 2) {
    std::string para1=argv[i];
    std::string para2=argv[i+1];
    if (para1.size() != 2 || para1[0] != '-') {
      Usage();
    }

    switch (para1[1]) {
      case 'c':   conf.num_of_clients = atoi(argv[i+1]);
                  break;
      case 'm':   conf.m = atoi(argv[i+1]);
                  break;
      case 'r':   conf.num_of_keys = atoi(argv[i+1]);
                  break;
      case 'n':    conf.num_of_requests = atoi(argv[i+1]);
                   break;
      case 'd':   conf.key_size = atoi(argv[i+1]);
                  break;
      case 's':   conf.value_size = atoi(argv[i+1]);
                  break;
      case 't':   {
                    std::string op;
                    for (unsigned int j = 0;j < para2.size(); j++) {
                      if (para2[j] != ',' && j < para2.size()-1) {
                        op += para2[j];
                      }
                      else {
                        if(j == para2.size()-1) {
                          op+=para2[j];
                        }
                        // std::transform(op.begin(), op.end(), op.begin(), std::tolower);
                        std::map<std::string,int>::iterator it_m=cmdMap.find(op);
                        if (it_m != cmdMap.end() ) {
                          conf.ops.push_back(it_m->second);
                          op="";
                        } else if (op.size() > 0) {
                          std::cout<<"can't find cmd:"<<op<<std::endl;
                          Usage();
                        }
                      }
                    }
                  }
                  break;
      default :    Usage();
                   break;
    }
  }
  if (conf.ops.size() <= 0) {
    Usage();
  }
}

void GetKVs(std::vector<nemo::KV> &kv, const std::vector<std::string> &key, const std::vector<std::string> &value) { 
  std::vector<std::string>::const_iterator it_k=key.begin();
  std::vector<std::string>::const_iterator it_v=value.begin();

  while (it_k != key.end() && it_v != value.end()) {
    nemo::KV temp;
    temp.key = *it_k++;
    temp.val = *it_v++;
    kv.push_back(temp);
  }
}


int main(int argc, char* argv[]) {

  int num_of_keys = 1000, num_of_clients = 1, num_of_requests = 1000;
  int key_size = 3, value_size = 3;
  int m = 10;

  Config conf;   
  conf.num_of_clients = num_of_clients;
  conf.num_of_requests = num_of_requests;
  conf.key_size = key_size; 
  conf.finished_requests = 0;
  conf.value_size = value_size;
  conf.num_of_keys = num_of_keys;
  conf.m = m;
  ParseOption(argc,argv,conf);
  conf.latency=std::vector<int64_t>(conf.num_of_requests+3*conf.num_of_clients,0);


  RandomString  rs; 
  conf.key.reserve(conf.num_of_keys);
  conf.value.reserve(conf.num_of_keys);
  conf.field.reserve(conf.num_of_keys);
  conf.kv.reserve(conf.num_of_keys);
  conf.ttl.reserve(conf.num_of_keys);
  rs.GetRandomKeyValues(conf.key, conf.value, conf.key_size, conf.value_size, conf.num_of_keys);
  GetKVs(conf.kv, conf.key, conf.value);
  rs.GetRandomFields(conf.field, conf.key_size, conf.num_of_keys);
  rs.GetRandomInts(conf.ttl, conf.num_of_keys);

  nemo::Options options;
  options.target_file_size_base = 20*1024*1024;  
  nemo::Nemo *np = new nemo::Nemo("./tmp",options);

  for (unsigned int i = 0;i < conf.ops.size(); i++) {
    conf.op=conf.ops[i];

    TestOPs(np,&conf);

    ShowReports(conf,"record");
    Reset(conf);
  }

  delete np;
  exit(0);
}



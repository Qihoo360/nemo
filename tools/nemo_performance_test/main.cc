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
using std::vector;

void usage(){
   std::cout<<"Usage: nemo-benchmark: -t <set,setttl,get...> [-c <clients>] [-r <keyspacelen>] [-n <requests>]\n"<<
		"      -c <clients>  number of clients(default 1)\n"<<
		"      -r <keyspacelen> number of random keys&values (default 1000)\n"<<
		"      -n <requests> total number of requests (default 100000) \n"<<
		"      -d <size> size of SET/GET key in bytes (default 3)\n"<<
		"      -s <size> size of SET/GET value in bytes (default 3)\n"<<
    "      -t <cmd> commend that you want to test\n";
   exit(0);
}

void parseOption(int argc,char *argv[],config &conf){
    if(argc%2==0){
	    usage();
    }
 
    std::map<std::string,int> cmdMap;    
    for(int j =0; j<cmdLen;j++){
      cmdMap.insert(std::pair<std::string,int>(cmd[j],j));
    }

    for(int i=1;i<argc;i+=2){
	    std::string para1=argv[i];
	    std::string para2=argv[i+1];
	    if(para1.size()!=2||para1[0]!='-'){
	      usage();
	    }

	    switch(para1[1]){
	      case 'c':   conf.numOfClients=atoi(argv[i+1]);
			               break;
	    
	      case 'r':   conf.numOfKeys=atoi(argv[i+1]);
			               break;
	      case 'n':    conf.numOfRequests=atoi(argv[i+1]);
			               break;
	      case 'd':    conf.playLoad=atoi(argv[i+1]);
			                break;
        case 's':    conf.valueSize=atoi(argv[i+1]);
                     break;
        case 't':   {
                     std::string op;
                     for(int j=0;j<para2.size();j++){
                        if(para2[j]!=','&&j<para2.size()-1){
                          op+=para2[j];
                        }
                        else{
                           if(j==para2.size()-1){
                             op+=para2[j];
                           }
                          // std::transform(op.begin(), op.end(), op.begin(), std::tolower);
                           std::map<std::string,int>::iterator it_m=cmdMap.find(op);
                           if(it_m!=cmdMap.end()){
                            conf.ops.push_back(it_m->second);
                            op="";
                          }
                        else if(op.size()>0){
                            std::cout<<"can't find cmd:"<<op<<std::endl;
                            usage();
                          }
                        }
                      }
                    }
                      break;
	      default :    usage();
			                 break;
	    }
    }
    if(conf.ops.size()<=0){
      usage();
    }
}


int main(int argc, char* argv[])
{
    int numOfKeys=1000,numOfClients=1,numOfRequests =1000;
    int playLoad=3,dataSize=3;
    
    config conf;   
    conf.numOfClients=numOfClients;
    conf.numOfRequests=numOfRequests;
    conf.playLoad=playLoad; 
    conf.finishedRequests=0;
    conf.valueSize=dataSize;
    conf.numOfKeys=numOfKeys;
    parseOption(argc,argv,conf);
    conf.latency=vector<int64_t>(conf.numOfRequests+3*conf.numOfClients,0);
    nemo::Options options;
    options.target_file_size_base=20*1024*1024;  
    nemo::Nemo *np=new nemo::Nemo("./tmp",options);
   
    random_string  rs; 
    conf.key.reserve(conf.numOfKeys);
    conf.value.reserve(conf.numOfKeys);
    conf.field.reserve(conf.numOfKeys);
    conf.kv.reserve(conf.numOfKeys);
    conf.ttl.reserve(conf.numOfKeys);
    rs.getRandomKeyValues(conf.key,conf.value,conf.playLoad,conf.valueSize,conf.numOfKeys);
    rs.getRandomKVs(conf.kv,conf.playLoad,conf.valueSize,conf.numOfKeys); 
    rs.getRandomFields(conf.field,conf.playLoad,conf.numOfKeys);
    rs.getRandomInts(conf.ttl,conf.numOfKeys);
   
    for(int i=0;i<conf.ops.size();i++){
      conf.op=conf.ops[i];
     
      test_ops(np,&conf);
     
      showReports(conf);
      reset(conf);
    }
 
    delete np;
    exit(0);
}



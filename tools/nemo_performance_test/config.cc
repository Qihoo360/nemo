
#include <iostream>
#include <ctime>
#include <stdint.h>
#include <limits>
#include <thread>
#include <algorithm>
#include <mutex>
#include <cstdlib>
#include "nemo.h"

#include "time_keeper.h"
#include "config.h"
#include "random_string.h"



#define PRE tk.restart()
#define POST int64_t delay=tk.delay_us(); \
             conf->mtx.lock(); count=++ conf->finishedRequests; conf->latency[count-1]=delay;\
             conf->mtx.unlock(); break

using namespace nemo;

template <typename T>
inline static void  buildVec(std::vector<T> &newVec,const std::vector<T> &srcVEc,int &start,int &len);
inline static void buildFV(std::vector<nemo::FV> &fv,const std::vector<std::string> &val,const std::vector<std::string> &field,int start,int len);

void showReports(const config &conf) {
    int count=0;
    std::cout<<conf.finishedRequests<<" requests completed in "<<conf.totalLatency*1.0/1000<<" seconds\n";
    std::cout<<conf.numOfClients<<" parallel clients\n";
    std::cout<<conf.playLoad<<" bytes keySize"<<std::endl;
    std::cout<<conf.valueSize<<"bytes keySize"<<std::endl;
    int curlat=0;
    float perc,qps;
    for(int i=0;i<conf.numOfRequests;i++){
        if(conf.latency[i]/500!=curlat||i==(conf.numOfRequests-1)){
            curlat=conf.latency[i]/500;
            perc=(i+1)*100.0/conf.numOfRequests;
	    if(curlat==0) curlat=1;
            std::cout<<perc<<"%<== "<<curlat*5<<" hundred microseconds\n";
        }
        count++;
     }
    std::cout<<conf.finishedRequests*1.0/(conf.totalLatency/1000.0)<<"  requests per seconds\n";
}

void reset(config &conf){
  conf.totalLatency=0;
  conf.finishedRequests=0;
  conf.latency=std::vector<int64_t>(conf.numOfRequests+3*conf.numOfClients,0);

}

void client_op(nemo::Nemo *np,config *conf){
  time_keeper tk;
  std::string val;
  std::vector<std::string> tempStr;
  std::vector<nemo::KV> kv;
  std::vector<nemo::KVS> kvss;
  std::vector<nemo::IV> ivs;
  std::vector<nemo::FV> fv;
  std::vector<nemo::FVS> fvs;
  std::vector<nemo::SM> sm;
  std::vector<double> weight(10,0.5);
  int64_t count=5, begin=0,end=10;
  int countInt=5;
  int32_t ttl=0;
  uint64_t limit=5;
  double score=20.5,dbegin=0.5,dend=10.5;
  int len=5;
  tk.restart();
  for(int count=conf->finishedRequests;count<conf->numOfRequests;){
    for(int index=0;count<conf->numOfRequests&&index<conf->numOfKeys;index++){
      switch(conf->op){
        //================KV=====================
        case 0:{  PRE; np->Set(conf->kv[index].key,conf->kv[index].val); POST; } 
        case 1:{  PRE; np->Set(conf->kv[index].key,conf->kv[index].val,conf->ttl[index]); POST; }
        case 2:{  PRE; np->Get(conf->kv[index].key,&val); POST;} 
        case 3:{  PRE; buildVec(kv,conf->kv,index,len);  np->MSet(kv); POST; }
        case 4:{  PRE; buildVec(tempStr,conf->key,index,len); np->MGet(tempStr, kvss); POST; } 
        case 5:{  PRE; buildVec(tempStr,conf->key,index,len); np->KMDel(tempStr, (int64_t*)&count); POST; }
        case 6:{  PRE; np->Incrby(conf->kv[index].key, count, conf->kv[index].val); POST; }
        case 7:{  PRE; np->Decrby(conf->kv[index].key, count, conf->kv[index].val); POST; }
        case 8:{  PRE; np->Incrbyfloat(conf->kv[index].key,score,conf->kv[index].val); POST;}
        case 9:{  PRE; np->GetSet(conf->kv[index].key,conf->kv[index].val,&val); POST;}
        case 10:{  PRE; np->Append(conf->kv[index].key,conf->kv[index].val,(int64_t*)&count); POST;}       
        case 11:{  PRE; np->Setnx(conf->kv[index].key,conf->kv[index].val,(int64_t*)&count); POST;}
        case 12:{  PRE; np->Setxx(conf->kv[index].key,conf->kv[index].val,(int64_t*)&count); POST;}        
        case 13:{  PRE; buildVec(kv,conf->kv,index,len); np->MSetnx(kv,(int64_t*)&count); POST;}
        case 14:{  PRE; np->Getrange(conf->kv[index].key,begin,end,conf->kv[index].key); POST;}
        case 15:{  PRE; np->Setrange(conf->kv[index].key,count,conf->kv[index].val,(int64_t*)&count); POST;}
        case 16:{  PRE; np->Strlen(conf->kv[index].key,(int64_t*)&count);  POST;}
        case 17:{  PRE; np->KScan(conf->kv[index].key,conf->kv[index].val,limit); POST;}
        case 18:{  PRE; np->Scan(count,conf->kv[index].key,count, tempStr,(int64_t*)&count); POST;}
        case 19:{  PRE; np->Keys(conf->kv[index].key,tempStr); POST;}

        //================BITMAP==================
        case 20:{ PRE; np->BitSet(conf->kv[index].key,begin,end,(int64_t*)&count); POST; }
        case 21:{ PRE; np->BitGet(conf->kv[index].key,end,(int64_t*)&count); POST; }
        case 22:{ PRE; np->BitCount(conf->kv[index].key,(int64_t*)&count);   POST;}
        case 23:{ PRE; np->BitPos(conf->kv[index].key,begin,end,(int64_t*)&count); POST;}
        case 24:{ PRE; BitOpType op=kBitOpOr; buildVec(tempStr,conf->key,index,len); np->BitOp(op,conf->kv[index].key,tempStr,(int64_t*)&count); POST;}
        case 25:{ PRE; np->SetWithExpireAt(conf->kv[index].key,conf->kv[index].val); POST;}
 
        //==================HASH====================
        case 26:{ PRE; np->HSet(conf->kv[index].key,conf->field[index],conf->kv[index].val); POST; }
        case 27:{ PRE; np->HGet(conf->kv[index].key,conf->field[index],&val); POST; } 
        case 28:{ PRE; np->HDel(conf->kv[index].key,conf->field[index]); POST;}
        case 29:{ PRE; np->HExists(conf->kv[index].key,conf->field[index]); POST;}
        case 30:{ PRE; np->HKeys(conf->kv[index].key,tempStr); POST;}
        case 31:{ PRE; fv.clear(); np->HGetall(conf->kv[index].key,fv); POST;}
        case 32:{ PRE; np->HLen(conf->kv[index].key); POST;}
        case 33:{ PRE;  buildFV(fv,conf->value,conf->field, index,len);
                     np->HMSet(conf->kv[index].key,fv); POST;}
        case 34:{ PRE; buildVec(tempStr,conf->key,index,len); np->HMGet(conf->kv[index].key,tempStr,fvs); POST;}
        case 35:{ PRE; np->HSetnx(conf->kv[index].key,conf->field[index],conf->kv[index].val); POST;}
        case 36:{ PRE; np->HStrlen(conf->kv[index].key,conf->field[index]); POST;}
        case 37:{ PRE; np->HScan(conf->kv[index].key,conf->kv[index].val,conf->field[index],limit); POST;}
        case 38:{ PRE; np->HVals(conf->kv[index].key,tempStr);  POST;}
        case 39:{ PRE; np->HIncrby(conf->kv[index].key,conf->field[index],end,val); POST;}
        case 40:{ PRE; np->HIncrbyfloat(conf->kv[index].key,conf->field[index],score,val); POST;}

        //=============List=========================
        case 41:{ PRE; np->LIndex(conf->kv[index].key,count,&val); POST; }
        case 42:{ PRE; np->LLen(conf->kv[index].key,(int64_t*)&count); POST;}
        case 43:{ PRE; np->LPush(conf->kv[index].key,conf->kv[index].val,(int64_t*)&count); POST;}
        case 44:{ PRE; np->LPop(conf->kv[index].key,&val); POST;}
        case 45:{ PRE; np->LPushx(conf->kv[index].key,conf->kv[index].val,(int64_t*)&count); POST;}
        case 46:{ PRE; np->LRange(conf->kv[index].key,begin,end,ivs); POST;}
        case 47:{ PRE; np->LSet(conf->kv[index].key,end,conf->kv[index].val); POST;}
        case 48:{ PRE; np->LTrim(conf->kv[index].key,begin,end); POST;}
        case 49:{ PRE; np->RPush(conf->kv[index].key,conf->kv[index].val,(int64_t*)&count); POST;}
        case 50:{ PRE; np->RPop(conf->kv[index].key,&val); POST;}
        case 51:{ PRE; np->RPushx(conf->kv[index].key,conf->kv[index].val,(int64_t*)&count); POST;}
        case 52:{ PRE; np->RPopLPush(conf->kv[index].key,conf->kv[index].val,conf->field[index]); POST;} 
        case 53:{ PRE; enum Position p=BEFORE; np->LInsert(conf->kv[index].key,p, conf->kv[index].val,conf->kv[index].val,(int64_t*)&count); POST;}
        case 54:{ PRE; np->LRem(conf->kv[index].key,count,conf->kv[index].val,(int64_t*)&count); POST;}


        //=============ZSet========================
        case 55:{ PRE; np->ZAdd(conf->kv[index].key,score,conf->kv[index].val,(int64_t*)&count); POST;}
        case 56:{ PRE; np->ZCard(conf->kv[index].key); POST;}
        case 57:{ PRE; np->ZCount(conf->kv[index].key,dbegin,dend); POST;}
        case 58:{ PRE; np->ZScan(conf->kv[index].key,dbegin,dend,limit); POST;}
        case 59:{ PRE; np->ZIncrby(conf->kv[index].key,conf->kv[index].val,score,val); POST;}
        case 60:{ PRE; np->ZRange(conf->kv[index].key,dbegin,dend, sm); POST;} 
       case 61:{ PRE; buildVec(tempStr,conf->key,index,countInt); enum Aggregate g=SUM; np->ZUnionStore(conf->kv[index].key,countInt,tempStr,weight,g,(int64_t*)&count); POST;}
        case 62:{ PRE; buildVec(tempStr,conf->key,index,countInt); enum Aggregate g=SUM; np->ZInterStore(conf->kv[index].key,countInt,tempStr,weight,g,(int64_t*)&count); POST;}
        case 63:{ PRE; np->ZRangebyscore(conf->kv[index].key,dbegin,dend,sm); POST;}
        case 64:{ PRE; np->ZRem(conf->kv[index].key,conf->kv[index].val,(int64_t*)&count); POST;}
        case 65:{ PRE; np->ZRank(conf->kv[index].key,conf->kv[index].val,(int64_t*)&count);  POST;}
        case 66:{ PRE; np->ZRevrank(conf->kv[index].key,conf->kv[index].val,(int64_t*)&count); POST;}
        case 67:{ PRE; np->ZScore(conf->kv[index].key,conf->kv[index].val,&score); POST;}
        case 68:{ PRE; np->ZRemrangebylex(conf->kv[index].key,conf->kv[index].val,conf->field[index],false,false,(int64_t*)&count); POST;}
        case 69:{ PRE; np->ZRemrangebyrank(conf->kv[index].key,begin,end,(int64_t*)&count); POST;}
        case 70:{ PRE; np->ZRemrangebyscore(conf->kv[index].key,dbegin,dend,(int64_t*)&count); POST;}
       
       //==============Set=======================
       case 71:{ PRE; np->SAdd(conf->kv[index].key,conf->kv[index].val,(int64_t*)&count); POST;}
       case 72:{ PRE; np->SRem(conf->kv[index].key,conf->kv[index].val,(int64_t*)&count); POST;}
       case 73:{ PRE; np->SCard(conf->kv[index].key); POST;}
       case 74:{ PRE; np->SScan(conf->kv[index].key,limit); POST;}
       case 75:{ PRE; np->SMembers(conf->kv[index].key,tempStr); POST;}
       case 76:{ PRE; np->SUnionStore(conf->kv[index].key,conf->key,(int64_t*)&count); POST;}
       case 77:{ PRE; np->SUnion(conf->key,tempStr); POST;}
       case 78:{ PRE; np->SInterStore(conf->kv[index].key,conf->key,(int64_t*)&count); POST;}
       case 89:{ PRE; np->SInter(conf->key,tempStr); POST;}
       case 80:{ PRE; np->SDiffStore(conf->kv[index].key,conf->key,(int64_t*)&count); POST;}
       case 81:{ PRE; np->SDiff(conf->key,tempStr); POST;}
       case 82:{ PRE; np->SIsMember(conf->kv[index].key,conf->kv[index].val); POST;}
       case 83:{ PRE; np->SPop(conf->kv[index].key,val); POST;}
       case 84:{ PRE; np->SRandMember(conf->kv[index].key,tempStr); POST;}
       case 85:{ PRE; np->SMove(conf->kv[index].key,conf->kv[index].val,conf->field[index],(int64_t*)&count); POST;}
      
       //=============HyperLogLog=================
       case 86:{ PRE; bool update=false; buildVec(tempStr,conf->value,index,len); np->PfAdd(conf->kv[index].key,tempStr,update); POST;}
       case 87:{ PRE; buildVec(tempStr,conf->key,index,len); np->PfCount(tempStr,count); POST;}
       case 88:{ PRE; buildVec(tempStr,conf->key,index,len); np->PfMerge(tempStr); POST;}
     
       //========default:Set====================
       default: {   exit(0);  break;} 
        
      }
    
    }
  
  }
}

void test_ops(nemo::Nemo *np,config *conf){
   std::vector<std::thread> pool;
   pool.reserve(conf->numOfClients*2);
   std::cout<<"running....."<<std::endl;
   time_keeper tk;
   for(int i=0;i<conf->numOfClients;i++){
	    pool.push_back(std::thread(client_op,np,conf));
   }
   for(int i=0;i<conf->numOfClients;i++){
    	pool[i].join();
   }
   conf->totalLatency=tk.delay_ms(); 
   std::sort(conf->latency.begin(),conf->latency.end());
}

template <typename T>
inline static void  buildVec(std::vector<T> &newVec,const std::vector<T> &srcVec,int &start,int &len){
  int length=srcVec.size();
  newVec.clear();
  newVec.resize(len);
  for(int i=0;i<len&&i+start<length;i++){
    newVec.push_back(srcVec[i+start]);
  }
}

inline static void buildFV(std::vector<nemo::FV> &fv,const std::vector<std::string> &val,const std::vector<std::string> &field,int start,int len){
  int length=val.size();
  fv.clear();
  for(int i=0;i<len&&i+start<length;i++){
    nemo::FV temp;
    temp.field=field[i+start];
    temp.val=val[i+start];
    fv.push_back(temp);
  }
}



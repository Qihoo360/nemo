#include <iostream>
#include <vector>
#include <string>
#include <ctime>
#include <inttypes.h>

#include "nemo.h"
#include "xdebug.h"

using namespace nemo;

int main()
{
    nemo::Options options;
    Nemo *n = new Nemo("./tmp", options); 
    Status s;

    n->StartBGThread();

    std::vector<uint64_t> nums;
    int64_t llen, za_res, sadd_res;

//    n->GetKeyNum(nums);
//
//    for (int i = 0; i < nums.size(); i++) {
//      printf (" i=%d %ld\n", i, nums[i]);
//    }
//
//    uint64_t num;
//    n->GetSpecifyKeyNum("kv", num);
//    printf ("kv key num: %lu\n", num);
//
//    n->GetSpecifyKeyNum("hash", num);
//    printf ("hash key num: %lu\n", num);
//
//    n->GetSpecifyKeyNum("list", num);
//    printf ("list key num: %lu\n", num);
//
//    n->GetSpecifyKeyNum("zset", num);
//    printf ("zset key num: %lu\n", num);
//
//    n->GetSpecifyKeyNum("set", num);
//    printf ("set key num: %lu\n", num);
//
//    s = n->GetSpecifyKeyNum("invalid type", num);
//    printf ("test invalid type:  ");
//    if (!s.ok()) {
//        printf ("SUCCESS, expect !ok\n");
//    } else {
//        printf ("FAILED, return ok, should failed\n");
//    }


    std::string res;

    /*
     * test CompactKey
     */
    
    for (int i = 0; i < 1; i++) {
      log_info("====== i: %3d ======", i);
      log_info("======Test CompactKey======");
      s = n->Set("key", "setval1");
      s = n->HSet("key", "hashfield1", "hashVal1");
      s = n->HSet("key", "hashfield2", "hashVal2");
      s = n->HSet("key", "hashfield3", "hashVal3");
      s = n->LPush("key", "tLPushVal1", &llen);
      s = n->LPush("key", "tLPushVal2", &llen);
      s = n->LPush("key", "tLPushVal3", &llen);
      s = n->ZAdd("key", 100.0, "zsetMember1", &za_res);
      s = n->ZAdd("key", 90.0, "zsetMember2", &za_res);
      s = n->ZAdd("key", 80.0, "zsetMember3", &za_res);
      s = n->SAdd("key", "member1", &sadd_res);
      s = n->SAdd("key", "member2", &sadd_res);
      s = n->SAdd("key", "member3", &sadd_res);

      log_info("");
      log_info("Before CompactKey");
      s = n->Get("key", &res);
      log_info("  KV: Get return %s, result res = %s", s.ToString().c_str(), res.c_str());

      for (int i = 0; i < llen; i++) {
        s = n->LIndex("key", i, &res);
        log_info("  List[%d] return %s, result res = %s", i, s.ToString().c_str(), res.c_str());
      }

      HIterator *hit = n->HScan("key", "", "", -1);
      if (hit == NULL) {
        log_info("  HScan error!");
      }
      for (; hit->Valid(); hit->Next()) {
        log_info("  HScan key: %s, field: %s, value: %s", hit->key().c_str(), hit->field().c_str(), hit->value().c_str());
      }
      log_info("");
      delete hit;


      ZIterator *zit = n->ZScan("key", ZSET_SCORE_MIN, ZSET_SCORE_MAX, -1);
      if (zit == NULL) {
        log_info("ZScan error!");
      }
      for (; zit->Valid(); zit->Next()) {
        log_info("  ZScan key: %s, score: %lf, member: %s", zit->key().c_str(), zit->score(), zit->member().c_str());
      }


      delete zit;

      int64_t del_ret;
      log_info("======Del key======");
      s = n->Del("key", &del_ret);
      log_info("  Del OK return %s, count is %lld", s.ToString().c_str(), del_ret);
    }

    
    char ch;
    scanf ("%c", &ch);

    return 0;
  
    /*
     * test Keys
     */
    log_info("======Test Keys======");
    s = n->Set("key", "setval1");
    s = n->HSet("key", "hashfield", "tSetVal1");
    s = n->LPush("key", "tLPushVal1", &llen);
    s = n->ZAdd("key", 100.0, "zsetMember1", &za_res);
    s = n->SAdd("key", "member1", &sadd_res);


    std::vector<std::string> keys;    
    std::vector<std::string>::iterator smit;    
    keys.clear();
    s = n->Keys("*", keys);
    log_info("Test Keys(\"*\") return %s, size=%ld", s.ToString().c_str(), keys.size());
    int i = 0;
    for (smit = keys.begin(); smit != keys.end(); smit++) {
        log_info(" %d : %s", i++, smit->c_str());
    }

    keys.clear();
    i = 0;
    s = n->Keys("*key*", keys);
    log_info("Test Keys(\"*key*\") return %s, size=%ld", s.ToString().c_str(), keys.size());
    for (smit = keys.begin(); smit != keys.end(); smit++) {
        log_info(" %d :  %s", i++, smit->c_str());
    }

    keys.clear();
    i = 0;
    s = n->Keys("t[HL]Set*", keys);
    log_info("Test Keys(\"t[HL]Set*\") return %s, size=%ld", s.ToString().c_str(), keys.size());
    for (smit = keys.begin(); smit != keys.end(); smit++) {
        log_info(" %d :  %s", i++, smit->c_str());
    }


    int64_t e_ret, ttl;

    log_info("======Test Expire======");
    s = n->Expire("key", 4, &e_ret);
    log_info("Test Expire with key=key in 7s, [hash, list, zset, set] return %s", s.ToString().c_str());

    for (int i = 0; i < 2; i++) {
        sleep(3);
        std::string res;

        s = n->Get("key", &res);
        log_info("          after %ds, Get return %s", (i+1)*3, s.ToString().c_str());

        s = n->HGet("key", "hashfield", &res);
        log_info("          after %ds, return %s", (i+1)*3, s.ToString().c_str());

        double score;
        s = n->ZScore("key", "zsetMember1", &score);
        log_info("          after %ds, ZScore return %s", (i+1)*3, s.ToString().c_str());

        int ret = n->SIsMember("key", "member1");
        log_info("          after %ds, SIsMember return %d, [true|false]", (i+1)*3, ret);

        s = n->LIndex("key", 0, &res);
        log_info("          after %ds, LIndex(0) return %s, val is %s", (i+1)*3, s.ToString().c_str(), res.c_str());

        if (s.ok()) {
            s = n->TTL("key", &ttl);
            log_info("          new TTL is %ld, TTL return %s\n", ttl, s.ToString().c_str());
        }
    }

    keys.clear();
    i = 0;
    s = n->Keys("*", keys);
    log_info("Test Keys(\"*\") return %s, size=%ld", s.ToString().c_str(), keys.size());
    for (smit = keys.begin(); smit != keys.end(); smit++) {
        log_info(" %d : %s", i++, smit->c_str());
    }

    delete n;
    return 0;
}

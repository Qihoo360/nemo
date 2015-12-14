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


    std::vector<uint64_t> nums;

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


    /*
     * test Keys
     */

    int64_t llen, za_res, sadd_res;
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

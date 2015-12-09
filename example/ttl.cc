#include <iostream>
#include <vector>
#include <string>
#include <ctime>
#include <inttypes.h>
#include <limits>

#include "nemo.h"
#include "xdebug.h"

using namespace nemo;

int main()
{
    nemo::Options options;
    options.target_file_size_base = 20 * 1024 * 1024;

    Nemo *n = new Nemo("./tmp/", options); 
    Status s;

    std::string res;

    /*
     *************************************************KV**************************************************
     */

    std::vector<std::string> keys;
    std::vector<KV> kvs;
    std::vector<KVS> kvss;
    std::vector<SM> sms;

    int64_t llen;
    int64_t za_res;
    int64_t sadd_res;

    /*
     *  Test MDel
     */
    log_info("======Test MDel======");
    s = n->Set("key", "setval1");
    s = n->HSet("key", "hashfield", "tSetVal1");
    s = n->LPush("key", "tLPushVal1", &llen);
    s = n->ZAdd("key", 100.0, "zsetMember1", &za_res);
    s = n->SAdd("key", "member1", &sadd_res);

    /*
     *  Test MDel
     */
    log_info("======Test MDel======");
    int64_t mcount;

    keys.push_back("key");
    s = n->MDel(keys, &mcount);
    log_info("Test MDel OK return %s", s.ToString().c_str());

    s = n->HGet("key", "hashfield", &res);
    log_info("        return %s", s.ToString().c_str());

    int64_t ret;
    double score;
    s = n->ZScore("key", "zsetMember1", &score);
    log_info("          ZScore return %s", s.ToString().c_str());

    ret = n->SIsMember("key", "member1");
    log_info("          SIsMember return %d, [true|false]", ret);

    s = n->LIndex("key", 0, &res);
    log_info("          LIndex(0) return %s, val is %s", s.ToString().c_str(), res.c_str());
    log_info("");

    /*
     *  Test TTL
     */
    log_info("======Test TTL======");
   // s = n->Set("key", "setval1");
    s = n->HSet("key", "hashfield", "tSetVal1");
    s = n->LPush("key", "tLPushVal1", &llen);
    s = n->ZAdd("key", 100.0, "zsetMember1", &za_res);
    s = n->SAdd("key", "member1", &sadd_res);

    /*
     *  Test Expire 
     */
    int64_t e_ret;
    int64_t ttl;
    log_info("======Test Expire======");
    s = n->Expire("key", 7, &e_ret);
    log_info("Test Expire with key=key in 7s, [hash, list, zset, set] return %s", s.ToString().c_str());

    for (int i = 0; i < 3; i++) {
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
    log_info("");

    /*
     *  Test Expireat
     */
    log_info("======Test HExpireat======");
    s = n->HSet("key", "hashfield", "tSetVal1");
    s = n->LPush("key", "tLPushVal1", &llen);
    s = n->ZAdd("key", 100.0, "zsetMember1", &za_res);
    s = n->SAdd("key", "member1", &sadd_res);

    std::time_t t = std::time(0);
    s = n->Expireat("key", t + 8, &e_ret);
    log_info("Test Expireat with key=tHSetKey at timestamp=%ld in 8s, return %s", (t+8), s.ToString().c_str());

    for (int i = 0; i < 3; i++) {
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
    log_info("");

    s = n->HSet("key", "hashfield", "tSetVal1");
    s = n->LPush("key", "tLPushVal1", &llen);
    s = n->ZAdd("key", 100.0, "zsetMember1", &za_res);
    s = n->SAdd("key", "member1", &sadd_res);

    s = n->Expireat("key", 8, &e_ret);
    log_info("\nTest Expireat with key=key at a passed timestamp=8, return %s", s.ToString().c_str());

    s = n->HGet("key", "hashfield", &res);
    log_info("        return %s", s.ToString().c_str());

    //double score;
    s = n->ZScore("key", "zsetMember1", &score);
    log_info("          ZScore return %s", s.ToString().c_str());

    ret = n->SIsMember("key", "member1");
    log_info("          SIsMember return %d, [true|false]", ret);

    s = n->LIndex("key", 0, &res);
    log_info("          LIndex(0) return %s, val is %s", s.ToString().c_str(), res.c_str());

    if (s.IsNotFound()) {
        n->TTL("key", &ttl);
        log_info("          NotFound key's TTL is %ld\n", ttl);
    }
    log_info("");

    /*
     *  Test Persist 
     */
    log_info("======Test Persist======");
    s = n->HSet("key", "hashfield", "tSetVal1");
    s = n->LPush("key", "tLPushVal1", &llen);
    s = n->ZAdd("key", 100.0, "zsetMember1", &za_res);
    s = n->SAdd("key", "member1", &sadd_res);
    s = n->Expire("key", 7, &e_ret);
    log_info("Test Persist with key=key in 7s, return %s", s.ToString().c_str());

    for (int i = 0; i < 3; i++) {
        sleep(3);
        if (i == 1) {
            s = n->Persist("key", &e_ret);
            log_info(" Test key return %s", s.ToString().c_str());
        }
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
    log_info("");

    delete n;

    return 0;
}

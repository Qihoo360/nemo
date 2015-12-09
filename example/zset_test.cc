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


    std::vector<std::string> keys;
    std::vector<KV> kvs;
    std::vector<KVS> kvss;
    std::vector<SM> sms;

    /*
     *************************************************ZSet**************************************************
     */
    /*
     *  Test Expire 
     */
    int64_t e_ret;
    int64_t ttl;
    int64_t zadd_res;

    s = n->ZAdd("tZSetKey", 0, "field11", &zadd_res);
    log_info("======ZAdd  return %s", s.ToString().c_str());
    log_info("  ======After zadd ZCard======");
    log_info("  Test ZCard, return card is %ld", n->ZCard("tZSetKey"));
    log_info("");

    log_info("======Test ZExpire======");
    s = n->ZExpire("tZSetKey", 7, &e_ret);
    log_info("Test ZExpire with key=tZSetKey in 7s, return %s", s.ToString().c_str());
    log_info("  ======After zexpire ZCard======");
    log_info("  Test ZCard, return card is %ld", n->ZCard("tZSetKey"));
    log_info("");

    for (int i = 0; i < 3; i++) {
        sleep(3);
        double score;
        s = n->ZScore("tZSetKey", "field11", &score);
        log_info("          after %ds, return %s", (i+1)*3, s.ToString().c_str());
        if (s.ok()) {
            s = n->ZTTL("tZSetKey", &ttl);
            log_info("          new ZTTL return %s, ttl is %ld, ZScore tZSetKey field11 score:%lf\n",
                     s.ToString().c_str(), ttl, score);
        }
    }
    log_info("");

    std::vector<SM>::iterator it_sm;

    for (int i = 0; i < 2; i++) {
      s = n->ZAdd("zr", 1, "a", &zadd_res);
      s = n->ZAdd("zr", 0.2, "b", &zadd_res);

      log_info("======Test ZDelKey zrange before======");
      s = n->ZRange("zr", 0, -1, sms);
      for (it_sm = sms.begin(); it_sm != sms.end(); it_sm++) {
        log_info("          score: %lf, member: %s", it_sm->score, it_sm->member.c_str());
      }
      log_info("");

      s = n->ZDelKey("zr");
      log_info("======ZDelKey  return %s", s.ToString().c_str());

      sms.clear();
      log_info("======Test ZDelKey zrange after======");
      s = n->ZRange("zr", 0, -1, sms);
      for (it_sm = sms.begin(); it_sm != sms.end(); it_sm++) {
        log_info("          score: %lf, member: %s", it_sm->score, it_sm->member.c_str());
      }
      log_info("");
    }

    /*
     *  Test Expireat
     */
    log_info("======Test ZExpireat======");
    s = n->ZAdd("tZSetKey", 0, "field12", &zadd_res);

    std::time_t t = std::time(0);
    s = n->ZExpireat("tZSetKey", t + 8, &e_ret);
    log_info("Test Expireat with key=tZSetKey at timestamp=%ld in 8s, return %s", (t+8), s.ToString().c_str());

    for (int i = 0; i < 3; i++) {
        sleep(3);
        double score;
        s = n->ZScore("tZSetKey", "field12", &score);
        log_info("          after %ds, return %s", (i+1)*3, s.ToString().c_str());
        if (s.ok()) {
            s = n->ZTTL("tZSetKey", &ttl);
            log_info("          new ZTTL return %s, ttl is %ld, ZScore tZSetKey field12 score:%lf\n",
                     s.ToString().c_str(), ttl, score);
        }
    }
    log_info("");

    s = n->ZAdd("tZSetKey", 0, "field12", &zadd_res);
    s = n->ZExpireat("tZSetKey", 8, &e_ret);
    log_info("Test ZExpireat with key=tZSetKey at a passed timestamp=8, return %s", s.ToString().c_str());
    double score;
    s = n->ZScore("tZSetKey", "field12", &score);
    log_info("          Get a invalid key return %s, expect ok",  s.ToString().c_str());
    if (s.IsNotFound()) {
        n->ZTTL("tZSetKey", &ttl);
        log_info("          NotFound key's TTL is %ld, ZScore score:%lf\n", ttl, score);
    }
    log_info("");

    /*
     *  Test Persist 
     */
    log_info("======Test ZPersist======");
    s = n->ZAdd("tZSetKey", 0, "field12", &zadd_res);
    s = n->ZExpire("tZSetKey", 7, &e_ret);
    log_info("Test ZPersist with key=tZSetKey in 7s, return %s", s.ToString().c_str());

    for (int i = 0; i < 3; i++) {
        sleep(3);
        if (i == 1) {
            s = n->ZPersist("tZSetKey", &e_ret);
            log_info(" Test ZPersist return %s", s.ToString().c_str());
        }
        double score;
        s = n->ZScore("tZSetKey", "field12", &score);
        log_info("          after %ds, return %s", (i+1)*3, s.ToString().c_str());
        if (s.ok()) {
            s = n->ZTTL("tZSetKey", &ttl);
            log_info("          new ZTTL return %s, ttl is %ld, ZScore tZSetKey field12 score:%lf\n",
                     s.ToString().c_str(), ttl, score);
        }
    }
    log_info("");
    delete n;

    return 0;
}

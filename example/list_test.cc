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

    /*
     *************************************************List**************************************************
     */
    std::vector<IV> ivs;

    /*
     *  Test LPush
     */
    log_info("======Test LPush======");
    int64_t llen = 0;
    s = n->LPush("tLPushKey", "tLPushVal1", &llen);
    log_info("Test LPush OK return %s, llen is %ld", s.ToString().c_str(), llen);
    s = n->LPush("tLPushKey", "tLPushVal2", &llen);
    log_info("Test LPush OK return %s, llen is %ld", s.ToString().c_str(), llen);
    s = n->LPush("tLPushKey", "tLPushVal3", &llen);
    log_info("Test LPush OK return %s, llen is %ld", s.ToString().c_str(), llen);
    s = n->LPush("tLPushKey", "tLPushVal4", &llen);
    log_info("Test LPush OK return %s, llen is %ld", s.ToString().c_str(), llen);
    s = n->LPush("tLPushKey", "tLPushVal5", &llen);
    log_info("Test LPush OK return %s, llen is %ld", s.ToString().c_str(), llen);
    s = n->LPush("tLPushKey", "tLPushVal6", &llen);
    log_info("Test LPush OK return %s, llen is %ld", s.ToString().c_str(), llen);
    log_info("");

    /*
     *  Test LLen
     */
    log_info("======Test LLen======");
    n->LLen("tLPushKey", &llen);
    log_info("Test LLen return %ld", llen);
    log_info("");

    /*
     *  Test LPop
     */
    log_info("======Test LPop======");
    res = "";
    s = n->LPop("tLPushKey", &res);
    log_info("Test LPop OK return %s, res = %s", s.ToString().c_str(), res.c_str());
    n->LLen("tLPushKey", &llen);
    log_info("After LPop, Test LLen return %ld", llen);
    log_info("");

  /*
   *  Test LPushx
   */
    log_info("======Test LPushx======");
    s = n->LPushx("tLPushKey", "tLpushval4", &llen);
    log_info("Test LPushx OK return %s", s.ToString().c_str());
    n->LLen("tLPushKey", &llen);
    log_info("After LPushx push an existed key, LLen return %ld", llen);

    s = n->LPushx("not-exist-key", "tLpushval4", &llen);
    log_info("Test LPushx push an non-exist key , LPushx OK return %s", s.ToString().c_str());
    n->LLen("tLPushKey", &llen);
    log_info("After LPushx push an non-exist key, LLen return %ld", llen);

    log_info("");

    /*
     *  Test LRange
     */
    log_info("======Test LRange======");
    ivs.clear();
    s = n->LRange("tLPushKey", 0, -1, ivs);
    log_info("Test LRange OK return %s", s.ToString().c_str());
    std::vector<IV>::iterator iter_iv;
    for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
        log_info("Test LRange index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
    }
    log_info("");

    /*
     *  Test LSet
     */
    log_info("======Test LSet======");
    s = n->LSet("tLPushKey", -2, "tLSetVal1");
    log_info("Test LSet OK return %s", s.ToString().c_str());
    ivs.clear();
    s = n->LRange("tLPushKey", 0, -1, ivs);
    for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
        log_info("After LSet, LRange index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
    }
    log_info("");

    /*
     *  Test LTrim
     */
    log_info("======Test LTrim======");
    s = n->LTrim("tLPushKey", -5, -5);
    log_info("Test LTrim OK return %s", s.ToString().c_str());
    ivs.clear();
    s = n->LRange("tLPushKey", 0, -1, ivs);
    for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
        log_info("After LTrim, LRange index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
    }
    log_info("");
    //just clear the list
    s = n->LTrim("tLPushKey", 1, 0);

    /*
     *  Test RPush
     */
    log_info("======Test RPush======");
    s = n->RPush("tLPushKey", "tLPushVal1", &llen);
    log_info("Test RPush OK return %s, llen is %ld", s.ToString().c_str(), llen);
    s = n->RPush("tLPushKey", "tLPushVal2", &llen);
    log_info("Test RPush OK return %s, llen is %ld", s.ToString().c_str(), llen);
    s = n->RPush("tLPushKey", "tLPushVal3", &llen);
    log_info("Test RPush OK return %s, llen is %ld", s.ToString().c_str(), llen);
    s = n->RPush("tLPushKey", "tLPushVal4", &llen);
    log_info("Test RPush OK return %s, llen is %ld", s.ToString().c_str(), llen);
    s = n->RPush("tLPushKey", "tLPushVal5", &llen);
    log_info("Test RPush OK return %s, llen is %ld", s.ToString().c_str(), llen);
    s = n->RPush("tLPushKey", "tLPushVal6", &llen);
    log_info("Test RPush OK return %s, llen is %ld", s.ToString().c_str(), llen);
    n->LLen("tLPushKey", &llen);
    log_info("After RPush LLen return %ld", llen);
    log_info("");
    
    /*
     *  Test RPop
     */
    log_info("======Test RPop======");
    res = "";
    s = n->RPop("tLPushKey", &res);
    log_info("Test RPop OK return %s, res = %s", s.ToString().c_str(), res.c_str());
    n->LLen("tLPushKey", &llen);
    log_info("After RPop, Test LLen return %ld", llen);
    ivs.clear();
    s = n->LRange("tLPushKey", 0, -1, ivs);
    for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
        log_info("After RPop, LRange index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
    }
    log_info("");


    /*
     *  Test RPushx
     */
    log_info("======Test RPushx======");
    s = n->RPushx("tLPushKey", "tLpushval4", &llen);
    log_info("Test RPushx OK return %s", s.ToString().c_str());
    n->LLen("tLPushKey", &llen);
    log_info("After RPushx push an existed key, LLen return %ld", llen);
    ivs.clear();
    s = n->LRange("tLPushKey", 0, -1, ivs);
    for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
        log_info("After RPushx push an existed key, LRange index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
    }

    s = n->LPushx("not-exist-key", "tLpushval4", &llen);
    log_info("Test RPushx push an non-exist key , LPushx OK return %s", s.ToString().c_str());
    n->LLen("tLPushKey", &llen);
    log_info("After RPushx push an non-exist key, LLen return %ld", llen);

    /*
     *  Test RPopLPush
     */
    log_info("======Test RPopLPush======");
    s = n->RPopLPush("tLPushKey", "tLPushKey", res);
    log_info("Test RPopLPush OK return %s", s.ToString().c_str());
    ivs.clear();
    s = n->LRange("tLPushKey", 0, -1, ivs);
    for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
        log_info("After RPushx push an existed key, LRange index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
    }

    //just clear the list
    s = n->LTrim("tLPushKey", 1, 0);
    log_info("");

    /*
     *  Test LInsert
     */
    log_info("======Test LInsert======");
    s = n->LTrim("tLPushKey", 1, 0);
    s = n->LTrim("tLPushKey2", 1, 0);
    s = n->LPush("tLPushKey", "tLPushVal1", &llen);
    s = n->LPush("tLPushKey", "tLPushVal2", &llen);

    ivs.clear();
    s = n->LRange("tLPushKey", 0, -1, ivs);
    log_info("   Test LInsert before,  LRange[0,-1] return %s", s.ToString().c_str());
    log_info("  insert between two keys, expect: [tLPushVal2 tLPushVal1]");
    for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
        log_info("    Test LInsert index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
    }

    s = n->LInsert("tLPushKey", AFTER, "tLPushVal2", "newkey", &llen);
    log_info("Test LInsert return %s", s.ToString().c_str());
    log_info("");

    ivs.clear();
    s = n->LRange("tLPushKey", 0, -1, ivs);
    log_info("Test LInsert after,  LRange[0,-1] return %s", s.ToString().c_str());
    log_info("  insert between two keys, expect: [tLPushVal2 newkey tLPushVal1]");
    for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
        log_info("   Test LInsert index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
    }
    log_info("");

    s = n->LPush("tLPushKey2", "tLPushVal1", &llen);
    s = n->LInsert("tLPushKey2", BEFORE, "tLPushVal1", "newkey", &llen);

    log_info("  insert before left, expect: [newkey tLPushVal1]");
    ivs.clear();
    s = n->LRange("tLPushKey2", 0, -1, ivs);
    log_info("Test LInsert OK return %s", s.ToString().c_str());
    for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
        log_info("Test LInsert index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
    }
    log_info("");

    s = n->LInsert("tLPushKey2", AFTER, "tLPushVal1", "newkeyright", &llen);

    log_info("  insert after right, expect: [newkey tLPushVal1 newkeyright]");
    ivs.clear();
    s = n->LRange("tLPushKey2", 0, -1, ivs);
    log_info("Test LInsert OK return %s", s.ToString().c_str());
    for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
        log_info("Test LInsert index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
    }
    log_info("");

    s = n->LInsert("tLPushKey2", AFTER, "newkeyright", "newkeyright2", &llen);

    log_info("  insert after right again, expect: [newkey tLPushVal1 newkeyright newkeyright2]");
    ivs.clear();
    s = n->LRange("tLPushKey2", 0, -1, ivs);
    log_info("Test LInsert OK return %s", s.ToString().c_str());
    for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
        log_info("Test LInsert index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
    }
    log_info("");

    log_info("======Test LRem==========================================================");
    s = n->LTrim("LRemKey", 1, 0);
  //s = n->LTrim("LRemKey2", 1, 0);
    s = n->LPush("LRemKey", "LRemVal1", &llen);
    s = n->LPush("LRemKey", "LRemVal2", &llen);
    s = n->LPush("LRemKey", "LRemVal1", &llen);
    s = n->LPush("LRemKey", "LRemVal2", &llen);
    s = n->LRem("LRemKey", 0, "LRemVal2", &llen);
    log_info("Test LRem OK return %s", s.ToString().c_str());

    ivs.clear();
    s = n->LRange("LRemKey", 0, -1, ivs);
    if (!s.ok()) {
      log_info("Test LRem LRange (0, -1) return %s", s.ToString().c_str());
    }
    log_info("  rem all LRemVal2, expect: [LRemVal1 LRemVal1]");
    for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
      log_info("Test LRem index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
    }
    log_info("");

    s = n->LRem("LRemKey", -1, "LRemVal1", &llen);
    log_info("Test LRem OK return %s", s.ToString().c_str());
    ivs.clear();
    s = n->LRange("LRemKey", 0, -1, ivs);
    if (!s.ok()) {
      log_info("Test LRem LRange (0, -1) return %s", s.ToString().c_str());
    }
    log_info("  rem -1 LRemVal1, expect: [LRemVal1]");
    for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
      log_info("Test LRem index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
    }
    log_info("");

    s = n->LRem("LRemKey", -1, "LRemVal1", &llen);
    log_info("Test LRem OK return %s", s.ToString().c_str());
    ivs.clear();
    s = n->LRange("LRemKey", 0, -1, ivs);
    if (!s.ok()) {
      log_info("Test LRem LRange (0, -1) return %s", s.ToString().c_str());
    }
    log_info("  rem -1 LRemVal1, expect: []");
    for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
      log_info("Test LRem index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
    }
    log_info("");


    s = n->LPush("LDelKey", "LDelVal1", &llen);
    s = n->LPush("LDelKey", "LDelVal2", &llen);
    log_info("  ======Before LDelKey======");
    log_info("  Test LPush OK return %s, llen is %ld", s.ToString().c_str(), llen);
    log_info("");

    int64_t e_ret;
    int64_t ttl;

    log_info("======Test LExpire======");
    s = n->LExpire("LDelKey", 7, &e_ret);
    log_info("Test LExpire with key=LDelKey in 7s, return %s", s.ToString().c_str());
    log_info("");

    for (int i = 0; i < 3; i++) {
        sleep(3);
        std::string val;
        s = n->LIndex("LDelKey", 0, &val);
        log_info("          after %ds, LIndex(0) return %s, val is %s", (i+1)*3, s.ToString().c_str(), val.c_str());
        if (s.ok()) {
            s = n->LTTL("LDelKey", &ttl);
            log_info("          new LTTL return %s, ttl is %ld\n",
                     s.ToString().c_str(), ttl);
        }
    }
    log_info("");

    std::vector<SM>::iterator it_sm;

    for (int i = 0; i < 2; i++) {
        s = n->LPush("zr", "LDelVal1", &llen);
        s = n->LPush("zr", "LDelVal2", &llen);

        log_info("======Test LDelKey Lrange before======");
        ivs.clear();
        s = n->LRange("zr", 0, -1, ivs);
        if (!s.ok()) {
          log_info("Test LRem LRange (0, -1) return %s", s.ToString().c_str());
        }
        log_info("   expect: [LRemVal2 LRemVal1]");
        for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
          log_info("     Test LRem index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
        }
        log_info("");


        int64_t del_ret;
        s = n->LDelKey("zr", &del_ret);
        log_info("======LDelKey  return %s", s.ToString().c_str());

        log_info("======Test LDelKey Lrange after======");
        ivs.clear();
        s = n->LRange("zr", 0, -1, ivs);
        if (!s.ok()) {
          log_info("Test LRem LRange (0, -1) return %s", s.ToString().c_str());
        }
        log_info("   expect null");
        for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
          log_info("     Test LRem index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
        }
        log_info("");
    }

    /*
     *  Test Expireat
     */
    log_info("======Test LExpireat======");
    s = n->LPush("tLPushKey", "tLPushVal1", &llen);

    std::time_t t = std::time(0);
    s = n->LExpireat("tLPushKey", t + 8, &e_ret);
    log_info("Test Expireat with key=tLPushKey at timestamp=%ld in 8s, return %s", (t+8), s.ToString().c_str());

    for (int i = 0; i < 3; i++) {
        sleep(3);
        std::string val;
        s = n->LIndex("tLPushKey", 0, &val);
        log_info("          after %ds, LIndex(0) return %s, val is %s", (i+1)*3, s.ToString().c_str(), val.c_str());
        if (s.ok()) {
            s = n->LTTL("tLPushKey", &ttl);
            log_info("          new LTTL return %s, ttl is %ld\n",
                     s.ToString().c_str(), ttl);
        }
    }

    log_info("");

    s = n->LPush("tLPushKey", "tLPushVal1", &llen);
    s = n->LExpireat("tLPushKey", 8, &e_ret);
    log_info("Test LExpireat with key=tLPushKey at a passed timestamp=8, return %s", s.ToString().c_str());

    std::string val;
    s = n->LIndex("tLPushKey", 0, &val);
    log_info("          LIndex (0) a invalid key return %s, expect ok",  s.ToString().c_str());
    if (s.IsNotFound()) {
        n->LTTL("tLPushKey", &ttl);
        log_info("          NotFound key's TTL is %ld\n", ttl);
    }
    log_info("");

    /*
     *  Test Persist 
     */
    log_info("======Test LPersist======");
    s = n->LPush("tLPushKey", "tLPushVal1", &llen);
    s = n->LExpire("tLPushKey", 7, &e_ret);
    log_info("Test LPersist with key=tLPushKey in 7s, return %s", s.ToString().c_str());

    for (int i = 0; i < 3; i++) {
        sleep(3);
        if (i == 1) {
            s = n->LPersist("tLPushKey", &e_ret);
            log_info(" Test LPersist return %s", s.ToString().c_str());
        }
        std::string val;
        s = n->LIndex("tLPushKey", 0, &val);
        log_info("          after %ds, LIndex(0) return %s, val is %s", (i+1)*3, s.ToString().c_str(), val.c_str());
        if (s.ok()) {
            s = n->LTTL("tLPushKey", &ttl);
            log_info("          new LTTL return %s, ttl is %ld\n",
                     s.ToString().c_str(), ttl);
        }
    }
    log_info("");

    delete n;

    return 0;
}

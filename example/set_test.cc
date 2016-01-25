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
    std::vector<std::string> values;
    int64_t za_res;

    /*
     ************************************************* Set **************************************************
     */

    /*
     *  Test SAdd
     */
    int64_t sadd_res;
    log_info("======Test SAdd======");
    s = n->SAdd("setKey", "member1", &sadd_res);
    log_info("Test SAdd OK return %s, sadd_res = %ld", s.ToString().c_str(), sadd_res);
    log_info("");
    std::string str1;
    s = n->SPop("setKey", str1);
    log_info("After SAdd, SPop get %s", str1.c_str());

    /*
     *  Test Scard
     */
    log_info("======Test SCard======");
    s = n->SAdd("setKey", "member1", &sadd_res);
    log_info("SCard with existed key return: %ld", n->SCard("setKey"));
    log_info("SCard with non-existe key return: %ld", n->SCard("non-exist"));
    log_info("");

    /*
     *  Test SScan
     */
    log_info("======Test SScan======");
    s = n->SAdd("setKey", "member1", &sadd_res);
    s = n->SAdd("setKey", "member2", &sadd_res);
    s = n->SAdd("setKey", "member3", &sadd_res);

    SIterator *sit = n->SScan("setKey", -1);
    if (sit == NULL) {
        log_info("SScan error!");
    }
    for (; sit->Valid(); sit->Next()) {
        log_info("SScan key: %s, member: %s", sit->key().c_str(), sit->member().c_str()); 
    }
    log_info("");

    /*
     *  Test SMembers
     */
    log_info("======Test SMembers======");
    values.clear();
    s = n->SMembers("setKey", values);
    log_info("Test SMembers return %s, expect [member1, member2, member3]", s.ToString().c_str());
    std::vector<std::string>::iterator smit;
    for (smit = values.begin(); smit != values.end(); smit++) {
        log_info("Test SMembers member: %s", smit->c_str());
    }
    log_info("");

    /*
     *  Test SUnion
     */
    log_info("======Test SUnion======");
    s = n->SAdd("unionKey1", "member1", &sadd_res);
    s = n->SAdd("unionKey1", "member2", &sadd_res);
    s = n->SAdd("unionKey1", "member3", &sadd_res);

    s = n->SAdd("unionKey2", "member21", &sadd_res);
    s = n->SAdd("unionKey2", "member2", &sadd_res);

    keys.clear();
    keys.push_back("unionKey1");
    keys.push_back("unionKey2");
    values.clear();
    s = n->SUnion(keys, values);
    log_info("Test SUnion[key1, key2] return %s, expect [member1, member2, member21, member3]", s.ToString().c_str());
    std::vector<std::string>::iterator suit;
    for (suit = values.begin(); suit != values.end(); suit++) {
        log_info("Test SUnion member: %s", suit->c_str());
    }
    log_info("");

    /*
     *  Test SUnionStore
     */
    log_info("======Test SUnionStore======");
    s = n->SAdd("unionKey1", "member1", &sadd_res);
    s = n->SAdd("unionKey1", "member2", &sadd_res);
    s = n->SAdd("unionKey1", "member3", &sadd_res);

    s = n->SAdd("unionKey2", "member21", &sadd_res);
    s = n->SAdd("unionKey2", "member2", &sadd_res);

    keys.clear();
    keys.push_back("unionKey1");
    keys.push_back("unionKey2");
    std::string dest = "dest";

    int64_t sus_res;
    s = n->SUnionStore(dest, keys, &sus_res);
    log_info("Test SUnionStore[dest, key1, key2] return %s, card is %ld, expect [member1, member2, member21, member3]", s.ToString().c_str(), sus_res);

    values.clear();
    s = n->SMembers(dest, values);
    for (suit = values.begin(); suit != values.end(); suit++) {
        log_info("Test SUnionStore member: %s", suit->c_str());
    }
    log_info("");

    s = n->SUnionStore("unionKey1", keys, &sus_res);
    log_info("Test SUnionStore[key1, key1, key2] return %s, card is %ld, expect key1=[member1, member2, member21, member3]", s.ToString().c_str(), sus_res);

    values.clear();
    s = n->SMembers("unionKey1", values);
    for (suit = values.begin(); suit != values.end(); suit++) {
        log_info("Test SUnionStore member: %s", suit->c_str());
    }
    log_info("");

    /*
     *  Test SInter
     */
    log_info("======Test SInter======");
    s = n->SAdd("interKey1", "member1", &sadd_res);
    s = n->SAdd("interKey1", "member2", &sadd_res);
    s = n->SAdd("interKey1", "member3", &sadd_res);

    s = n->SAdd("interKey2", "member21", &sadd_res);
    s = n->SAdd("interKey2", "member2", &sadd_res);

    keys.clear();
    keys.push_back("interKey1");
    keys.push_back("interKey2");
    values.clear();
    s = n->SInter(keys, values);
    log_info("Test SInter[key1, key2] return %s, expect [member2]", s.ToString().c_str());
    for (suit = values.begin(); suit != values.end(); suit++) {
        log_info("Test SInter member: %s", suit->c_str());
    }
    log_info("");

    /*
     *  Test SInterStore
     */
    log_info("======Test SInterStore======");
    s = n->SAdd("interKey1", "member1", &sadd_res);
    s = n->SAdd("interKey1", "member2", &sadd_res);
    s = n->SAdd("interKey1", "member3", &sadd_res);

    s = n->SAdd("interKey2", "member21", &sadd_res);
    s = n->SAdd("interKey2", "member2", &sadd_res);

    keys.clear();
    keys.push_back("interKey1");
    keys.push_back("interKey2");
    dest = "dest";

    int64_t sis_res;
    s = n->SInterStore(dest, keys, &sis_res);
    log_info("Test SInterStore[dest, key1, key2] return %s, card is %ld, expect [member2]", s.ToString().c_str(), sis_res);

    values.clear();
    s = n->SMembers(dest, values);
    for (suit = values.begin(); suit != values.end(); suit++) {
        log_info("Test SInterStore member: %s", suit->c_str());
    }
    log_info("");

    s = n->SInterStore("interKey1", keys, &sis_res);
    log_info("Test SInterStore[key1, key1, key2] return %s, card is %ld, expect key1=[member2]", s.ToString().c_str(), sis_res);

    values.clear();
    s = n->SMembers("interKey1", values);
    for (suit = values.begin(); suit != values.end(); suit++) {
        log_info("Test SInterStore member: %s", suit->c_str());
    }
    log_info("");

    /*
     *  Test SDiff
     */
    log_info("======Test SDiff======");
    s = n->SAdd("diffKey1", "member1", &sadd_res);
    s = n->SAdd("diffKey1", "member2", &sadd_res);
    s = n->SAdd("diffKey1", "member3", &sadd_res);

    s = n->SAdd("diffKey2", "member21", &sadd_res);
    s = n->SAdd("diffKey2", "member2", &sadd_res);

    keys.clear();
    keys.push_back("diffKey1");
    keys.push_back("diffKey2");
    values.clear();
    s = n->SDiff(keys, values);
    log_info("Test SDiff[key1, key2] return %s, expect [member1, member3]", s.ToString().c_str());
    for (suit = values.begin(); suit != values.end(); suit++) {
        log_info("Test SDiff member: %s", suit->c_str());
    }
    log_info("");

    /*
     *  Test SDiffStore
     */
    log_info("======Test SDiffStore======");
    s = n->SAdd("diffKey1", "member1", &sadd_res);
    s = n->SAdd("diffKey1", "member2", &sadd_res);
    s = n->SAdd("diffKey1", "member3", &sadd_res);

    s = n->SAdd("diffKey2", "member21", &sadd_res);
    s = n->SAdd("diffKey2", "member2", &sadd_res);

    keys.clear();
    keys.push_back("diffKey1");
    keys.push_back("diffKey2");
    dest = "dest";

    int64_t sds_res;
    s = n->SDiffStore(dest, keys, &sds_res);
    log_info("Test SDiffStore[dest, key1, key2] return %s, card is %ld, expect [member1, member3]", s.ToString().c_str(), sds_res);

    values.clear();
    s = n->SMembers(dest, values);
    for (suit = values.begin(); suit != values.end(); suit++) {
        log_info("Test SDiffStore member: %s", suit->c_str());
    }
    log_info("");

    s = n->SDiffStore("diffKey1", keys, &sds_res);
    log_info("Test SDiffStore[key1, key1, key2] return %s, card is %ld, expect key1=[member1, member3]", s.ToString().c_str(), sds_res);

    values.clear();
    s = n->SMembers("diffKey1", values);
    for (suit = values.begin(); suit != values.end(); suit++) {
        log_info("Test SDiffStore member: %s", suit->c_str());
    }
    log_info("");

    /*
     *  Test SRandMember
     */
    log_info("======Test SRandMember======");
    s = n->SAdd("randKey1", "member1", &sadd_res);
    s = n->SAdd("randKey1", "member2", &sadd_res);
    s = n->SAdd("randKey1", "member3", &sadd_res);
    s = n->SAdd("randKey1", "member4", &sadd_res);

    values.clear();
    s = n->SRandMember("randKey1", values, 5);
    log_info("Test SRandMember(5) no-repeated return %s, expect {member[1-4]}", s.ToString().c_str());
    for (suit = values.begin(); suit != values.end(); suit++) {
        log_info("Test SRandMember member: %s", suit->c_str());
    }
    log_info("");

    values.clear();
    s = n->SRandMember("randKey1", values, -5);
    log_info("Test SRandMember(-5) allow repeated return %s, expect 5 random members", s.ToString().c_str());
    for (suit = values.begin(); suit != values.end(); suit++) {
        log_info("Test SRandMember member: %s", suit->c_str());
    }
    log_info("");

    /*
     *  Test SPop
     */
    log_info("======Test SPop======");
    s = n->SAdd("popKey1", "member1", &sadd_res);
    s = n->SAdd("popKey1", "member2", &sadd_res);
    s = n->SAdd("popKey1", "member3", &sadd_res);
    s = n->SAdd("popKey1", "member4", &sadd_res);

    values.clear();
    std::string pop_string;
    s = n->SPop("popKey1", pop_string);
    log_info("Test SPop() return %s, pop = (%s), expect random member{member[1-4]}", s.ToString().c_str(), pop_string.c_str());

    values.clear();
    s = n->SMembers("popKey1", values);
    log_info(" After SPop, SMembers return %s, expect 3 members remains", s.ToString().c_str());
    for (suit = values.begin(); suit != values.end(); suit++) {
        log_info("Test SPop member: %s", suit->c_str());
    }
    log_info("");

    /*
     *  Test SIsMember
     */
    log_info("======Test SIsMember======");
    s = n->SAdd("setKey1", "member1", &sadd_res);
    log_info("Test SIsMember with exist member return %d, expect [true]", n->SIsMember("setKey", "member1"));
    log_info("Test SIsMember with non-exist member return %d, expect [false]", n->SIsMember("setKey", "non-member"));
    log_info("Test SIsMember with non-exist key return %d, expect [false]", n->SIsMember("setKeyasdf", "member"));

    /*
     *  Test SRem
     */
    log_info("======Test SRem======");
    int64_t srem_res;
    s = n->SRem("setKey", "member2", &srem_res);
    log_info("Test SRem with exist member return %s, expect [member1, member3]", s.ToString().c_str());

    values.clear();
    s = n->SMembers("setKey", values);
    for (smit = values.begin(); smit != values.end(); smit++) {
        log_info("    member: %s", smit->c_str());
    }
    log_info("");

    s = n->SRem("setKey", "member2", &srem_res);
    log_info("Test SRem with nonexist member return %s, rem_res is %ld expect [member1, member3]", s.ToString().c_str(), srem_res);
    values.clear();
    s = n->SMembers("setKey", values);
    for (smit = values.begin(); smit != values.end(); smit++) {
        log_info("    member: %s", smit->c_str());
    }
    log_info("");

    /*
     *  Test SMove
     */
    log_info("======Test SMove======");
    s = n->SAdd("moveKey1", "member1", &sadd_res);
    s = n->SAdd("moveKey1", "member2", &sadd_res);
    s = n->SAdd("moveKey2", "member1", &sadd_res);

    int64_t smove_res;
    s = n->SMove("moveKey1", "moveKey2", "member2", &smove_res);
    log_info("Test SMove(key1, key2, member2) return %s, expect key1=%ld [member1]  key2= %ld [member1, member2]",
             s.ToString().c_str(), n->SCard("moveKey1"), n->SCard("moveKey2"));

    values.clear();
    s = n->SMembers("moveKey1", values);
    for (smit = values.begin(); smit != values.end(); smit++) {
        log_info("    moveKey1 member: %s", smit->c_str());
    }
    values.clear();
    s = n->SMembers("moveKey2", values);
    for (smit = values.begin(); smit != values.end(); smit++) {
        log_info("    moveKey2 member: %s", smit->c_str());
    }
    log_info("");

    s = n->SAdd("tSetKey", "field11", &sadd_res);
    log_info("======SAdd  return %s", s.ToString().c_str());
    log_info("  ======After sadd SCard======");
    log_info("  Test SCard, return card is %ld", n->SCard("tSetKey"));
    log_info("");

    int64_t e_ret;
    log_info("======Test SExpire======");
    s = n->SExpire("tSetKey", 7, &e_ret);
    log_info("Test SExpire with key=tSetKey in 7s, return %s", s.ToString().c_str());
    log_info("  ======After sexpire SCard======");
    log_info("  Test SCard, return card is %ld", n->SCard("tSetKey"));
    log_info("");

    int64_t ttl;
    for (int i = 0; i < 3; i++) {
        sleep(3);
        int ret = n->SIsMember("tSetKey", "field11");
        log_info("          after %ds, SIsMember return %d, [true|false]", (i+1)*3, ret);
        if (ret) {
            s = n->STTL("tSetKey", &ttl);
            log_info("          new STTL return %s, ttl is %ld\n", s.ToString().c_str(), ttl);
        }
    }
    log_info("");

    std::vector<SM>::iterator it_sm;

    for (int i = 0; i < 2; i++) {
      s = n->SAdd("zr", "a", &sadd_res);
      s = n->SAdd("zr", "b", &sadd_res);

      log_info("======Test SDelKey SMembers before======");
      values.clear();
      s = n->SMembers("zr", values);
      for (smit = values.begin(); smit != values.end(); smit++) {
        log_info("    zr member: %s", smit->c_str());
      }
      log_info("");

      int64_t del_ret;
      s = n->SDelKey("zr", &del_ret);
      log_info("======SDelKey  return %s", s.ToString().c_str());

      log_info("======Test SDelKey SMembers after======");
      values.clear();
      s = n->SMembers("zr", values);
      for (smit = values.begin(); smit != values.end(); smit++) {
        log_info("    zr member: %s", smit->c_str());
      }
      log_info("");
    }

    /*
     *  Test Expireat
     */
    log_info("======Test SExpireat======");
    s = n->SAdd("tSetKey", "field12", &sadd_res);
    //s = n->SAdd("setKey", "member1", &sadd_res);

    std::time_t t = std::time(0);
    s = n->SExpireat("tSetKey", t + 8, &e_ret);
    log_info("Test Expireat with key=tSetKey at timestamp=%ld in 8s, return %s", (t+8), s.ToString().c_str());

    for (int i = 0; i < 3; i++) {
        sleep(3);
        int ret = n->SIsMember("tSetKey", "field12");
        log_info("          after %ds, SIsMember return %d, [true|false]", (i+1)*3, ret);
        if (ret) {
            s = n->STTL("tSetKey", &ttl);
            log_info("          new STTL return %s, ttl is %ld\n", s.ToString().c_str(), ttl);
        }
    }
    log_info("");

    s = n->SAdd("tSetKey", "field12", &sadd_res);
    s = n->SExpireat("tSetKey", 8, &e_ret);
    log_info("Test ZExpireat with key=tSetKey at a passed timestamp=8, return %s", s.ToString().c_str());
    int ret = n->SIsMember("tSetKey", "field12");
    log_info("      Get a invalid key return %d, [1|0] expect false", ret);
    if (s.IsNotFound()) {
        n->STTL("tSetKey", &ttl);
        log_info("          NotFound key's TTL is %ld\n", ttl);
    }
    log_info("");

    /*
     *  Test Persist 
     */
    log_info("======Test SPersist======");
    s = n->SAdd("tSetKey", "field12", &sadd_res);
    s = n->SExpire("tSetKey", 7, &e_ret);
    log_info("Test SPersist with key=tSetKey in 7s, return %s", s.ToString().c_str());

    for (int i = 0; i < 3; i++) {
        sleep(3);
        if (i == 1) {
            s = n->SPersist("tSetKey", &e_ret);
            log_info(" Test SPersist return %s", s.ToString().c_str());
        }
        int ret = n->SIsMember("tSetKey", "field12");
        log_info("          after %ds, SIsMember return %d, [true|false]", (i+1)*3, ret);
        if (ret) {
            s = n->STTL("tSetKey", &ttl);
            log_info("          new STTL return %s, ttl is %ld\n", s.ToString().c_str(), ttl);
        }
    }
    log_info("");
    delete n;

    return 0;
}

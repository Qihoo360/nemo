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
     *  Test Set
     */
    log_info("======Test Set======");
    s = n->Set("tSetKey", "tSetVal");
    log_info("Test Set OK return %s", s.ToString().c_str());
    log_info("");

    /*
     *  Test Set with TTL
     */
    log_info("======Test Set======");
    s = n->Set("tSetKeyWithTTL", "tSetVal", 7);
    log_info("Test Set with ttl return %s", s.ToString().c_str());

    int64_t ttl;
    for (int i = 0; i < 3; i++) {
        sleep(3);
        s = n->Get("tSetKeyWithTTL", &res);
        log_info("          Set with ttl after %ds, return %s", (i+1)*3, s.ToString().c_str());
        if (s.ok()) {
            n->TTL("tSetKeyWithTTL", &ttl);
            log_info("          new TTL is %ld, Get res:%s\n", ttl, res.c_str());
        }
    }
    log_info("");

    /*
     *  Test Get
     */
    log_info("======Test Get======");
    s = n->Set("tGetKey", "tGetVal");
    res = "";
    s = n->Get("tGetKey", &res);
    log_info("Test Get OK return %s, result tGetVal = %s", s.ToString().c_str(), res.c_str());
    res = "";
    s = n->Get("tGetNotFoundKey", &res);
    log_info("Test Get NotFound return %s, result NULL = %s", s.ToString().c_str(), res.c_str());
    log_info("");

    /*
     *  Test Setxx
     */
    log_info("======Test Setxx======");
    s = n->Del("a");
    int64_t xx_ret;
    s = n->Setxx("a", "b", &xx_ret);
    log_info("Test Setxx OK return %s, retval: %ld", s.ToString().c_str(), xx_ret);
    s = n->Get("a", &res);
    log_info("After setxx, Get return %s, result res = %s", s.ToString().c_str(), res.c_str());
    /*
     *  Test Expire 
     */
    int64_t e_ret;
    log_info("======Test Expire======");
    s = n->Expire("tSetKey", 7, &e_ret);
    log_info("Test Expire with key=tSetKey in 7s, return %s", s.ToString().c_str());

    for (int i = 0; i < 3; i++) {
        sleep(3);
        s = n->Get("tSetKey", &res);
        log_info("          after %ds, return %s", (i+1)*3, s.ToString().c_str());
        if (s.ok()) {
            n->TTL("tSetKey", &ttl);
            log_info("          new TTL is %ld, Get res:%s\n", ttl, res.c_str());
        }
    }
    log_info("");


    /*
     *  Test Expireat
     */
    log_info("======Test Expireat======");
    s = n->Set("tSetKey", "tSetVal");

    std::time_t t = std::time(0);
    s = n->Expireat("tSetKey", t + 8, &e_ret);
    log_info("Test Expireat with key=tSetKey at timestamp=%ld in 8s, return %s", (t+8), s.ToString().c_str());

    for (int i = 0; i < 3; i++) {
        sleep(3);
        s = n->Get("tSetKey", &res);
        log_info("          after %ds, return %s", (i+1)*3, s.ToString().c_str());
        if (s.ok()) {
            n->TTL("tSetKey", &ttl);
            log_info("          new TTL is %ld, Get res:%s\n", ttl, res.c_str());
        }
    }
    log_info("");

    s = n->Set("tSetKey", "tSetVal");
    s = n->Expireat("tSetKey", 8, &e_ret);
    log_info("Test Expireat with key=tSetKey at a passed timestamp=8, return %s", s.ToString().c_str());
    s = n->Get("tSetKey", &res);
    log_info("          Get a invalid key return %s, expect ok",  s.ToString().c_str());
    if (s.IsNotFound()) {
        n->TTL("tSetKey", &ttl);
        log_info("          NotFound key's TTL is %ld, Get res:%s\n", ttl, res.c_str());
    }
    log_info("");

    /*
     *  Test Persist 
     */
    log_info("======Test Persist======");
    s = n->Set("tSetKey", "tSetVal");
    s = n->Expire("tSetKey", 7, &e_ret);
    log_info("Test Persist with key=tSetKey in 7s, return %s", s.ToString().c_str());

    for (int i = 0; i < 3; i++) {
        sleep(3);
        if (i == 1) {
            s = n->Persist("tSetKey", &e_ret);
            log_info(" Test Persist return %s", s.ToString().c_str());
        }
        s = n->Get("tSetKey", &res);
        log_info("          after %ds, return %s", (i+1)*3, s.ToString().c_str());
        if (s.ok()) {
            n->TTL("tSetKey", &ttl);
            log_info("          new TTL is %ld, Get res:%s\n", ttl, res.c_str());
        }
    }
    log_info("");
    /*
     *  Test Del
     */
    log_info("======Test Del======");
    s = n->Del("tSetKey");
    log_info("Test Det OK return %s", s.ToString().c_str());
    res = "";
    s = n->Get("tSetKey", &res);
    log_info("After Del, Get NotFound return %s, result tGetVal = %s", s.ToString().c_str(), res.c_str());

    //just delete all key-value set before
    s = n->Del("tGetKey");
    log_info("");

    /*
     *  Test MGet
     */
    log_info("======Test MGet======");
    keys.clear();
    kvss.clear();
    keys.push_back("tMGetKey1");
    keys.push_back("tMGetKey2");
    keys.push_back("tMGetNotFoundKey");
    s = n->Set("tMGetKey1", "tMGetVal1");
    s = n->Set("tMGetKey2", "tMGetVal1");

    s = n->MGet(keys, kvss);
    std::vector<KVS>::iterator kvs_iter;
    for (kvs_iter = kvss.begin(); kvs_iter != kvss.end(); kvs_iter++) {
        log_info("Test MGet OK return %s, key: %s, val: %s status: %s", s.ToString().c_str(), kvs_iter->key.c_str(), kvs_iter->val.c_str(), kvs_iter->status.ToString().c_str());
    }
    log_info("");

    /*
     *  Test MDel
     */
    log_info("======Test MDel======");
    int64_t mcount;
    s = n->MDel(keys, &mcount);
    log_info("Test MDel OK return %s", s.ToString().c_str());
    //After MDel, all should return NotFound
    kvss.clear();
    s = n->MGet(keys, kvss);
    for (kvs_iter = kvss.begin(); kvs_iter != kvss.end(); kvs_iter++) {
        log_info("After MDel, Test MGet OK return %s, key: %s, val: %s status: %s", s.ToString().c_str(), kvs_iter->key.c_str(), kvs_iter->val.c_str(), kvs_iter->status.ToString().c_str());
    }
    log_info("");

    /*
     *  Test MSet
     */
    log_info("======Test MSet======");
    kvs.clear();
    kvs.push_back({"tMSetKey1", "tMSetVal1mm"});
    kvs.push_back({"tMSetKey2", "tMSetVal2mm"});
    kvs.push_back({"tMSetKey3", "tMSetVal3mm"});
    s = n->MSet(kvs);
    log_info("Test MSet OK return %s", s.ToString().c_str());

    keys.clear();
    keys.push_back("tMSetKey1");
    keys.push_back("tMSetKey2");
    keys.push_back("tMSetKey3");
    kvss.clear();
    s = n->MGet(keys, kvss);
    for (kvs_iter = kvss.begin(); kvs_iter != kvss.end(); kvs_iter++) {
        log_info("After MSet, Test MGet OK return %s, key: %s, val: %s status: %s", s.ToString().c_str(), kvs_iter->key.c_str(), kvs_iter->val.c_str(), kvs_iter->status.ToString().c_str());
    }

    //just delete all key-value set before
    s = n->MDel(keys, &mcount);
    log_info("");

    /*
     *  Test Incrby
     */
    log_info("======Test Incrby======");
    s = n->Set("tIncrByKey", "012");
    res = "";
    s = n->Incrby("tIncrByKey", 6, res);
    log_info("Test Incrby OK return %s, 12 Incrby 6 val: %s", s.ToString().c_str(), res.c_str());
    res = "";
    s = n->Incrby("tIncrByKey", -2, res);
    log_info("Test Incrby OK return %s, 18 Incrby -2 val: %s", s.ToString().c_str(), res.c_str());

    s = n->Incrby("tIncrByKey", LLONG_MAX, res);
    log_info("Test Incrby OK return %s, Incrby LLONG_MAX, expect overflow", s.ToString().c_str());

    //Test NonNum key IncrBy
    s = n->Set("tIncrByKey", "NonNum");
    res = "";
    s = n->Incrby("tIncrByKey", 6, res);
    log_info("Test Incrby OK return %s, NonNum Incrby 6 val: %s", s.ToString().c_str(), res.c_str());

    /*
     *  Test Decrby
     */
    log_info("======Test Decrby======");
    s = n->Set("tIncrByKey", "012");
    res = "";
    s = n->Decrby("tIncrByKey", 6, res);
    log_info("Test Decrby OK return %s, 12 Decrby 6 val: %s", s.ToString().c_str(), res.c_str());
    res = "";
    s = n->Decrby("tIncrByKey", -2, res);
    log_info("Test Decrby OK return %s, 6 Decrby -2 val: %s", s.ToString().c_str(), res.c_str());

    s = n->Decrby("tIncrByKey", LLONG_MIN, res);
    log_info("Test Decrby OK return %s, Decrby LLONG_MIN, expect overflow", s.ToString().c_str());

    //Test NonNum key IncrBy
    s = n->Set("tIncrByKey", "NonNum");
    res = "";
    s = n->Decrby("tIncrByKey", 6, res);
    log_info("Test Decrby OK return %s, NonNum Decrby 6 val: %s", s.ToString().c_str(), res.c_str());

    //just delete all key-value set before
    s = n->Del("tIncrByKey");
    log_info("");

    /*
     *  Test Incrbyfloat
     */
    log_info("======Test Incrbyfloat======");
    s = n->Set("tIncrByKey", "012");
    res = "";
    s = n->Incrbyfloat("tIncrByKey", 6.0, res);
    log_info("Test Incrbyfloat OK return %s, 12 Incrbyfloat 6.0 val: %s", s.ToString().c_str(), res.c_str());
    res = "";
    s = n->Incrbyfloat("tIncrByKey", -2.3, res);
    log_info("Test Incrbyfloat OK return %s, 18 Incrbyfloat -2.3 val: %s", s.ToString().c_str(), res.c_str());

    s = n->Incrbyfloat("tIncrByKey", std::numeric_limits<double>::max(), res);
    s = n->Incrbyfloat("tIncrByKey", std::numeric_limits<double>::max(), res);
    log_info("Test Incrbyfloat OK return %s, Incrbyfloat numric_limits max , expect overflow", s.ToString().c_str());

    //Test NonNum key IncrBy
    s = n->Set("tIncrByKey", "NonNum");
    res = "";
    s = n->Incrbyfloat("tIncrByKey", 6, res);
    log_info("Test Incrbyfloat OK return %s, NonNum Incrbyfloat 6 expect value not float", s.ToString().c_str());

    //just delete all key-value set before
    s = n->Del("tIncrByKey");
    log_info("");
    //char ch;
    //scanf ("%c", &ch);
    
    /*
     *  Test GetSet
     */
    log_info("======Test GetSet======");
    s = n->Set("tGetSetKey", "tGetSetVal");
    res = "";
    s = n->GetSet("tGetSetKey", "tGetSetVal_new", &res);
    log_info("Test GetSet OK return %s, old_val: %s", s.ToString().c_str(), res.c_str());
    res = "";
    s = n->Get("tGetSetKey", &res);
    log_info("After GetSet, Test Get OK return %s, val: %s", s.ToString().c_str(), res.c_str());

    // test NonExist key GetSet;
    res = "";
    s = n->GetSet("tGetSetNotFoundKey", "tGetSetNotFoundVal_new", &res);
    log_info("Test GetSet OK return %s, old_val: %s", s.ToString().c_str(), res.c_str());
    s = n->Get("tGetSetNotFoundKey", &res);
    log_info("After GetSet, Test Get OK return %s, val: %s", s.ToString().c_str(), res.c_str());

    //just delete all key-value set before
    s = n->Del("tGetSetKey");
    s = n->Del("tGetSetNotFoundKey");
    log_info("");

    /*
     *  Test Getrange
     */
    log_info("======Test Getrange======");
    s = n->Set("tGetrangekey", "abcd");
    std::string substr;
    s = n->Getrange("tGetrangekey", 0, -1, substr);
    log_info("substr: %s", substr.c_str());

    
    /*
     *  Test Scan
     */
    log_info("======Test Scan======");
    kvs.clear();
    kvs.push_back({"tScanKey1", "tScanVal1"});
    kvs.push_back({"tScanKey2", "tScanVal2"});
    kvs.push_back({"tScanKey21", "tScanVal21"});
    kvs.push_back({"tScanKey3", "tScanVal3"});
    
    keys.clear();
    keys.push_back("tScanKey1");
    keys.push_back("tScanKey2");
    keys.push_back("tScanKey2");

    s = n->MSet(kvs);
    KIterator *scan_iter = n->Scan("tScanKey1", "tScanKey2", -1);
    if (scan_iter == NULL) {
        log_info("Scan error!");
    }
    while (scan_iter->Next()) {
        log_info("Test Scan key: %s, value: %s", scan_iter->Key().c_str(), scan_iter->Val().c_str());
    }

    //just delete all key-value set before
    s = n->MDel(keys, &mcount);
    log_info("");

    res = "";
    keys.clear();
    kvs.clear();
    kvss.clear();



    delete n;

    return 0;
}

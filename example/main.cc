#include "nemo.h"
#include "xdebug.h"

#include <vector>
#include <string>

using namespace nemo;

int main()
{
    Nemo *n = new Nemo("./tmp/"); 
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
    s = n->MDel(keys);
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
    kvs.push_back({"tMSetKey1", "tMSetVal1"});
    kvs.push_back({"tMSetKey2", "tMSetVal2"});
    kvs.push_back({"tMSetKey3", "tMSetVal3"});
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
    s = n->MDel(keys);
    log_info("");

    /*
     *  Test Incrby
     */
    log_info("======Test Incrby======");
    s = n->Set("tIncrByKey", "12");
    res = "";
    s = n->Incrby("tIncrByKey", 6, res);
    log_info("Test Incrby OK return %s, 12 Incrby 6 val: %s", s.ToString().c_str(), res.c_str());
    res = "";
    s = n->Incrby("tIncrByKey", -2, res);
    log_info("Test Incrby OK return %s, 18 Incrby -2 val: %s", s.ToString().c_str(), res.c_str());

    //Test NonNum key IncrBy
    s = n->Set("tIncrByKey", "NonNum");
    res = "";
    s = n->Incrby("tIncrByKey", 6, res);
    log_info("Test Incrby OK return %s, NonNum Incrby 6 val: %s", s.ToString().c_str(), res.c_str());

    //just delete all key-value set before
    s = n->Del("tIncrByKey");
    log_info("");
    
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
    s = n->MDel(keys);
    log_info("");

    res = "";
    keys.clear();
    kvs.clear();
    kvss.clear();

    /*
     *************************************************HASH**************************************************
     */
    std::vector<std::string> fields;
    std::vector<std::string> values;
    std::vector<FV> fvs;
    std::vector<FVS> fvss;
    
    /*
     *  Test HSet
     */
    log_info("======Test HSet======");
    s = n->HSet("tHSetKey", "song", "tHSetVal");
    log_info("Test HSet OK return %s", s.ToString().c_str());
    log_info("");

    /*
     *  Test HGet
     */
    log_info("======Test HGet======");
    s = n->HSet("tHGetKey", "song", "tGetVal");
    res = "";
    s = n->HGet("tHGetKey", "song", &res);
    log_info("Test HGet OK return %s, result tHGetVal = %s", s.ToString().c_str(), res.c_str());
    res = "";
    s = n->HGet("tHGetNotFoundKey", "song", &res);
    log_info("Test Get NotFound return %s, result NULL = %s", s.ToString().c_str(), res.c_str());
    s = n->HGet("tHGetKey", "non-field", &res);
    log_info("Test Get NotFound return %s, result NULL = %s", s.ToString().c_str(), res.c_str());
    log_info("");

    /*
     *  Test HDel
     */
    log_info("======Test HDel======");
    s = n->HDel("tHSetKey", "song");
    log_info("Test HDet OK return %s", s.ToString().c_str());
    res = "";
    s = n->HGet("tHSetKey", "song", &res);
    log_info("After HDel, HGet NotFound return %s, result tGetVal = %s", s.ToString().c_str(), res.c_str());

    //just delete all key-value set before
    s = n->HDel("tHGetKey", "song");
    log_info("");

    /*
     *  Test HExists
     */
    log_info("======Test HExists======");
    s = n->HSet("tHExistsKey", "song", "tHExistsVal");
    log_info("test HExists with existed key & field return: %d", n->HExists("tHExistsKey", "song"));
    log_info("test HExists with non-existed key return: %d", n->HExists("tHExistsNonKey", "song"));
    log_info("test HExists with non-existed fields return: %d", n->HExists("tHExistsKey", "non-field"));
    
    //just delete all key-value set before
    s = n->HDel("tHExistsKey", "song");
    log_info("");

    /*
     *  Test HStrlen
     */
    log_info("======Test HStrlen======");
    s = n->HSet("tHStrlenKey", "song", "tHStrlenVal");
    log_info("test HStrlen return 11 = %ld", n->HStrlen("tHStrlenKey", "song"));
    
    //just delete all key-value set before
    s = n->HDel("tHStrlenKey", "song");
    log_info("");

    /*
     *  Test HSetnx
     */
    log_info("======Test HSetnx======");
    s = n->HSetnx("tHSetnxKey", "song", "tHSetnxVal");
    log_info("test HSetnx with non-existed key | fields return %s", s.ToString().c_str());
    s = n->HSetnx("tHSetnxKey", "song", "tHSetnxVal");
    log_info("test HSetnx with existed key | fields return %s", s.ToString().c_str());

    //just delete all key-value set before
    s = n->HDel("tHSetnxKey", "song");
    log_info("");

    /*
     *  Test HIncrby
     */
    log_info("======Test HIncrby======");
    s = n->HSet("tHIncrByKey", "song", "12");
    res = "";
    s = n->HIncrby("tHIncrByKey", "song", 6, res);
    log_info("Test HIncrby OK return %s, 12 HIncrby 6 val: %s", s.ToString().c_str(), res.c_str());
    res = "";
    s = n->HIncrby("tHIncrByKey", "song", -2, res);
    log_info("Test HIncrby OK return %s, 18 HIncrby -2 val: %s", s.ToString().c_str(), res.c_str());

    //Test NonNum key HIncrBy
    s = n->HSet("tHIncrByKey", "song", "NonNum");
    res = "";
    s = n->HIncrby("tHIncrByKey", "song", 6, res);
    log_info("Test HIncrby OK return %s, NonNum HIncrby 6 val: %s", s.ToString().c_str(), res.c_str());

    //just delete all key-value set before
    s = n->HDel("tHIncrByKey", "song");
    log_info("");

    /*
     *  Test HIncrbyfloat
     */
    log_info("======Test HIncrbyfloat======");
    s = n->HSet("tHIncrByfloatKey", "song", "12.1");
    res = "";
    s = n->HIncrbyfloat("tHIncrByfloatKey", "song", 6.2, res);
    log_info("Test HIncrbyfloat OK return %s, 12.1 HIncrbyfloat 6.2 val: %s", s.ToString().c_str(), res.c_str());
    res = "";
    s = n->HIncrbyfloat("tHIncrByfloatKey", "song", -2.1, res);
    log_info("Test HIncrbyfloat OK return %s, 18.3 HIncrbyfloat -2.1 val: %s", s.ToString().c_str(), res.c_str());
    res = "";
    s = n->HIncrbyfloat("tHIncrByfloatKey", "nonExist", 5.0, res);
    log_info("Test HIncrbyfloat OK return %s, nonExist HIncrbyfloat 5.0 val: %s", s.ToString().c_str(), res.c_str());

    //Test NonNum key HIncrBy
    s = n->HSet("tHIncrByfloatKey", "song", "NonNum");
    res = "";
    s = n->HIncrby("tHIncrByfloatKey", "song", 6, res);
    log_info("Test HIncrbyfloat OK return %s, NonNum HIncrbyfloat 6 val: %s", s.ToString().c_str(), res.c_str());
    log_info("");

    /*
     *  Test HMGet
     */
    log_info("======Test MGet======");
    fields.clear();
    fvss.clear();
    fields.push_back("tHMGetField1");
    fields.push_back("tHMGetField2");
    fields.push_back("tHMGetField3");
    fields.push_back("tHMGetNotFoundField");
    s = n->HSet("tHMGetKey", "tHMGetField1", "tHMGetVal1");
    s = n->HSet("tHMGetKey", "tHMGetField2", "tHMGetVal2");
    s = n->HSet("tHMGetKey", "tHMGetField3", "tHMGetVal3");

    s = n->HMGet("tHMGetKey", fields, fvss);
    std::vector<FVS>::iterator fvs_iter;
    for (fvs_iter = fvss.begin(); fvs_iter != fvss.end(); fvs_iter++) {
        log_info("Test HMGet OK return %s, field: %s, val: %s status: %s", s.ToString().c_str(), fvs_iter->field.c_str(), fvs_iter->val.c_str(), fvs_iter->status.ToString().c_str());
    }
    log_info("");

    /*
     *  Test HKeys
     */
    log_info("======Test HKeys======");
    fields.clear();
    s = n->HKeys("tHMGetKey", fields);
    log_info("Test Hkey OK return %s", s.ToString().c_str());
    std::vector<std::string>::iterator field_iter;
    for (field_iter = fields.begin(); field_iter != fields.end(); field_iter++) {
        log_info("Test HKeys field: %s", (*field_iter).c_str());
    }
    log_info("");

    /*
     *  Test HGetall
     */
    log_info("======Test HGetall======");
    fvs.clear();
    s = n->HGetall("tHMGetKey", fvs);
    log_info("Test HGetall OK return %s", s.ToString().c_str());
    std::vector<FV>::iterator fv_iter;
    for (fv_iter = fvs.begin(); fv_iter != fvs.end(); fv_iter++) {
        log_info("Test HGetall, field: %s, val: %s", fv_iter->field.c_str(), fv_iter->val.c_str());
    }
    log_info("");
    
    /*
     *  Test HVals
     */
    log_info("======Test HVals======");
    values.clear();
    s = n->HVals("tHMGetKey", values);
    log_info("Test HVals OK return %s", s.ToString().c_str());
    std::vector<std::string>::iterator value_iter;
    for (value_iter = values.begin(); value_iter != values.end(); value_iter++) {
        log_info("Test HVals field: %s", (*value_iter).c_str());
    }
    log_info("");
    
    /*
     *  Test HLen
     */
    log_info("======Test HLen======");
    log_info("HLen with existed key return: %ld", n->HLen("tHMGetKey"));
    log_info("HLen with non-existe key return: %ld", n->HLen("non-exist"));
    log_info("");

    /*
     *  Test HScan
     */
    log_info("======Test HScan======");

    HIterator *hit = n->HScan("tHMGetKey", "", "", -1);
    if (hit == NULL) {
        log_info("HScan error!");
    }
    while (hit->Next()) {
        log_info("HScan key: %s, field: %s, value: %s", hit->Key().c_str(), hit->Field().c_str(), hit->Val().c_str());
    }
    log_info("");
    //just delete all key-value set before
    s = n->HDel("tHMGetKey1", "tHMGetVal1");
    s = n->HDel("tHMGetKey2", "tHMGetVal2");
    s = n->HDel("tHMGetKey3", "tHMGetVal3");

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
    s = n->LPush("tLPushKey", "tLPushVal2", &llen);
    s = n->LPush("tLPushKey", "tLPushVal3", &llen);
    s = n->LPush("tLPushKey", "tLPushVal4", &llen);
    s = n->LPush("tLPushKey", "tLPushVal5", &llen);
    s = n->LPush("tLPushKey", "tLPushVal6", &llen);
    log_info("Test LPush OK return %s", s.ToString().c_str());
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
    s = n->RPush("tLPushKey", "tLPushVal2", &llen);
    s = n->RPush("tLPushKey", "tLPushVal3", &llen);
    s = n->RPush("tLPushKey", "tLPushVal4", &llen);
    s = n->RPush("tLPushKey", "tLPushVal5", &llen);
    s = n->RPush("tLPushKey", "tLPushVal6", &llen);
    log_info("Test RPush OK return %s", s.ToString().c_str());
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

    log_info("======Test LInsert======");
    s = n->LTrim("tLPushKey", 1, 0);
    s = n->LTrim("tLPushKey2", 1, 0);
    s = n->LPush("tLPushKey", "tLPushVal1", &llen);
    s = n->LPush("tLPushKey", "tLPushVal2", &llen);
    s = n->LInsert("tLPushKey", AFTER, "tLPushVal2", "newkey", &llen);

    ivs.clear();
    s = n->LRange("tLPushKey", 0, -1, ivs);
    log_info("Test LInsert OK return %s", s.ToString().c_str());
    log_info("  insert between two keys, expect: [tLPushVal2 newkey tLPushVal1]");
    for (iter_iv = ivs.begin(); iter_iv != ivs.end(); iter_iv++) {
        log_info("Test LInsert index =  %ld, val = %s", (*(iter_iv)).index, (*(iter_iv)).val.c_str());
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
    /*
     *************************************************ZSet**************************************************
     */

    /*
     *  Test ZAdd
     */
    log_info("======Test ZAdd======");
    int64_t zadd_res;
    s = n->ZAdd("tZAddKey", 0, "tZAddMem0", &zadd_res);
    s = n->ZAdd("tZAddKey", 1, "tZAddMem1", &zadd_res);
    s = n->ZAdd("tZAddKey", 1, "tZAddMem1_2", &zadd_res);
    s = n->ZAdd("tZAddKey", 2, "tZAddMem2", &zadd_res);
    s = n->ZAdd("tZAddKey", 2, "tZAddMem2_2", &zadd_res);
    s = n->ZAdd("tZAddKey", 3, "tZAddMem3", &zadd_res);
    s = n->ZAdd("tZAddKey", 7, "tZAddMem7", &zadd_res);
    log_info("Test ZAdd OK return %s", s.ToString().c_str());
    log_info("");

    /*
     *  Test ZCard
     */
    log_info("======Test ZCard======");
    log_info("Test ZCard, return %ld", n->ZCard("tZAddKey"));
    log_info("");
    
    /*
     *  Test ZScan
     */
    log_info("======Test Zscan======");
    ZIterator *it_zset = n->ZScan("tZAddKey", 0, 10, -1);
    if (it_zset == NULL) {
        log_info("ZScan error!");
    }
    while (it_zset->Next()) {
        log_info("Test ZScan key: %s, score: %ld, member: %s", it_zset->Key().c_str(), it_zset->Score(), it_zset->Member().c_str());
    }

    /*
     *  Test ZCount
     */
    log_info("======Test ZCount======");
    log_info("Test ZCount, return %ld", n->ZCount("tZAddKey", -1, 3)); 
    log_info("");

    /*
     *  Test ZIncrby
     */
    log_info("======Test ZIncrby======");
    s = n->ZIncrby("tZAddKey", "tZAddMem1", 5);
    log_info("Test ZIncrby with exist key OK return %s", s.ToString().c_str());
    s = n->ZIncrby("tZAddKey", "tZAddMem_ne", 7);
    log_info("Test ZIncrby with non-exist key OK return %s", s.ToString().c_str());
    it_zset = n->ZScan("tZAddKey", 0, 100, -1);
    if (it_zset == NULL) {
        log_info("ZScan error!");
    }
    while (it_zset->Next()) {
        log_info("After ZIncrby, Scan key: %s, score: %ld, value: %s", it_zset->Key().c_str(), it_zset->Score(), it_zset->Member().c_str());
    }
    log_info("After ZIncrby, ZCard return %ld", n->ZCard("tZAddKey")); 
    log_info("");

    /*
     *  Test ZRangebyscore
     */
    log_info("======Test ZRangebyscore======");
    sms.clear();
    s = n->ZRangebyscore("tZAddKey", 2, 6, sms);
    std::vector<SM>::iterator it_sm;
    for (it_sm = sms.begin(); it_sm != sms.end(); it_sm++) {
        log_info("Test ZRangebyscore score: %ld, member: %s", it_sm->score, it_sm->member.c_str());
    }

    return 0;
}

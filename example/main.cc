#include "nemo.h"
#include "xdebug.h"

#include <vector>
#include <string>

using namespace nemo;

int main()
{
    rocksdb::Options opt;
    opt.create_if_missing = true;
    Nemo *n = new Nemo("./tmp/db", opt); 
    rocksdb::Status s;

    std::string res;

    /*
     *************************************************KV**************************************************
     */

    std::vector<std::string> keys;
    std::vector<KV> kvs;
    std::vector<KVS> kvss;

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
    for(kvs_iter = kvss.begin(); kvs_iter != kvss.end(); kvs_iter++) {
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
    for(kvs_iter = kvss.begin(); kvs_iter != kvss.end(); kvs_iter++) {
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
    for(kvs_iter = kvss.begin(); kvs_iter != kvss.end(); kvs_iter++) {
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
    kvs.push_back({"tScanKey3", "tScanVal3"});
    
    keys.clear();
    keys.push_back("tScanKey1");
    keys.push_back("tScanKey2");
    keys.push_back("tScanKey2");

    s = n->MSet(kvs);
    KIterator *scan_iter = n->scan("", "", -1);
    if(scan_iter == NULL){
        log_info("Scan error!");
    }
    while(scan_iter->next()){
        log_info("Test Scan key: %s, value: %s", scan_iter->key.c_str(), scan_iter->val.c_str());
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
    for(fvs_iter = fvss.begin(); fvs_iter != fvss.end(); fvs_iter++) {
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
    for(field_iter = fields.begin(); field_iter != fields.end(); field_iter++) {
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
    for(fv_iter = fvs.begin(); fv_iter != fvs.end(); fv_iter++) {
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
    for(value_iter = values.begin(); value_iter != values.end(); value_iter++) {
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
    if(hit == NULL){
        log_info("HScan error!");
    }
    while(hit->next()){
        log_info("HScan key: %s, field: %s, value: %s", hit->key.c_str(), hit->field.c_str(), hit->val.c_str());
    }
    log_info("");
    //just delete all key-value set before
    s = n->HDel("tHMGetKey1", "tHMGetVal1");
    s = n->HDel("tHMGetKey2", "tHMGetVal2");
    s = n->HDel("tHMGetKey3", "tHMGetVal3");
    














/*
    s = n->MSet(kvs);
    s = n->HMSet("zhao", kvs);
    log_info("HMSet return %s", s.ToString().c_str());
    s = n->Del("eyfield1");
    s = n->Del("noexist");
    s = n->Set("songzhao", "lucky");
    s = n->Set("songzhao1", "lucky1");
    s = n->Set("songzhao2", "lucky2");
    s = n->Get("songzhao", &res);
    s = n->GetSet("notexist", "uu", &res);
    log_info("GetSet return %s, old_val: %s", s.ToString().c_str(), res.c_str());
    KIterator *it = n->scan("", "", -1);
    if(it == NULL){
        log_info("Scan error!");
    }
    int num = 0;
    while(it->next()){
        log_info("key: %s, value: %s", it->key.c_str(), it->val.c_str());
        num++;
    }
    log_info("num = %d", num);
    
    s = n->HSet("song", "cong", "8");
    log_info("HSet return: %s", s.ToString().c_str());
    s = n->HSet("song", "zhao", "6");
    log_info("HSet return: %s", s.ToString().c_str());
    log_info("HSize return %ld", n->HSize("song"));
    s = n->HGet("song", "zhao", &res);
    log_info("HGet return: %s, val %s", s.ToString().c_str(), res.c_str());
    s = n->HDel("song", "zhao");
    log_info("HDel return: %s", s.ToString().c_str());
    s = n->HGet("song", "zhao", &res);
    log_info("HGet return: %s, val %s", s.ToString().c_str(), res.c_str());
    log_info("HExists return: %d", n->HExists("song", "zhao"));
    log_info("HExists return: %d", n->HExists("song", "cong"));
    
    s = n->HSet("song", "yuan", "16");
    std::vector<std::string> vec_keys;
    s = n->HKeys("song", vec_keys);
    std::vector<std::string>::iterator it1;
    for(it1 = vec_keys.begin(); it1 != vec_keys.end(); it1++) {
        log_info("HKeys : %s", (*it1).c_str());
    }
    kvs.clear();
    s = n->HGetall("song", kvs);
    std::vector<Kv>::iterator it2;
    for(it2 = kvs.begin(); it2 != kvs.end(); it2++) {
        log_info("HGetall : %s %s", (*it2).key.c_str(), (*it2).val.c_str());
    }

    log_info("HLen return: %ld", n->HLen("song"));
    log_info("HLen return: %ld", n->HLen("zhao"));
    keys.push_back("key7");
    keys.push_back("key8");
    keys.push_back("key9");
    kvss.clear();
    s = n->HMGet("zhao", keys, kvss);
    for(iter = kvss.begin(); iter != kvss.end(); iter++) {
        log_info("HMGet : %s, %s, %s", iter->key.c_str(), iter->val.c_str(), iter->status.ToString().c_str());
    }
    HIterator *hit = n->HScan("zhao", "", "key8", -1);
    if(hit == NULL){
        log_info("Scan error!");
    }
    num = 0;
    while(hit->next()){
        log_info("key: %s, field: %s, value: %s", hit->key.c_str(), hit->field.c_str(), hit->val.c_str());
        num++;
    }
    log_info("num = %d", num);
    n->HDel("song", "shirley");
    s = n->HSetnx("song", "shirley", "688");
    log_info("HSetnx return %s", s.ToString().c_str());
    s = n->HSetnx("song", "shirley", "688");
    log_info("HSetnx return %s", s.ToString().c_str());

    log_info("HStrlen return %ld", n->HStrlen("zhao", "key7"));
    log_info("HStrlen return %ld", n->HStrlen("song", "fuk"));

    std::vector<std::string> values;
    s = n->HVals("zhao", values);
    std::vector<std::string>::iterator vit;
    for(vit = values.begin(); vit != values.end(); vit++) {
        log_info("HVals %s", (*vit).c_str());
    }

    s = n->HIncrby("song", "shirley", -2, res);
    log_info("Incrby return %s, new_val = %s", s.ToString().c_str(), res.c_str());
    s = n->HIncrby("song", "shir", 6, res);
    log_info("Incrby return %s, new_val = %s", s.ToString().c_str(), res.c_str());
*/
    return 0;
}

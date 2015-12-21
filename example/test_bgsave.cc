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
    Nemo *n = new Nemo("./test_dir1/dir2/dir3", options); 
    Status s;

    s = n->Set("tSetKey1", "tSetVal1");
    s = n->Set("tSetKey2", "tSetVal2");
    s = n->Set("tSetKey3", "tSetVal3");

    s = n->Set("tSetKey4", "tSetVal4", 300);
    sleep(2);

    s = n->HSet("tHSetKey", "member1", "value1");
    s = n->HSet("tHSetKey", "member2", "value2");
    s = n->HSet("tHSetKey", "member3", "value3");

    s = n->HSet("tHSetKeyTTL", "member1", "value1");
    
    int64_t za_res;
    s = n->ZAdd("key", 100.0, "zsetMember1", &za_res);
    s = n->ZAdd("key", 100.0, "zsetMember2", &za_res);

    int64_t sadd_res;
    s = n->SAdd("key", "member1", &sadd_res);
    s = n->SAdd("key", "member2", &sadd_res);

    int64_t llen;
    s = n->LPush("tLPushKey", "tLPushVal1", &llen);
    s = n->LPush("tLPushKey", "tLPushVal2", &llen);
    s = n->LPush("tLPushKey", "tLPushVal3", &llen);
    s = n->LPush("tLPushKey", "tLPushVal4", &llen);
    s = n->LPush("tLPushKey", "tLPushVal5", &llen);
    s = n->LPush("tLPushKey", "tLPushVal6", &llen);

    std::string res = "";
    int64_t ttl;

    /*
     *  Test LLen
     */
    n->LLen("tLPushKey", &llen);
    log_info("Origin LLen return %ld", llen);
    log_info("Origin ZCard, return %ld", n->ZCard("key"));
    log_info("Origin SCard, return %ld", n->SCard("key"));
    log_info("");

    std::vector<FV> fvs;
    std::vector<const rocksdb::Snapshot*> snapshots;
    std::vector<FV>::iterator fv_iter;

//    log_info("================");
//    log_info("======Test Dumpall ======");
//    s = n->BGSaveGetSnapshot(snapshots);
//    log_info("Get Snapshots return %s, expect size=5, result size=%lu", s.ToString().c_str(), snapshots.size());
//    //for (int i = 0; i < snapshots.size(); i++) {
//    //    printf ("%d: seqnumber=%d\n", i, snapshots[i]->GetSequenceNumber());
//    //}
//
//    s = n->Set("tSetKey1", "tSetVal1AfterSnapshot");
//    s = n->LPush("tLPushKey", "tLPushVal7AfterSnapshot", &llen);
//    s = n->HSet("tHSetKey", "member1", "value1AfterSnapshot");
//    s = n->BGSave(snapshots);
//
//
//
//    Nemo *n1 = new Nemo("./dump/", options); 
//     
//    s = n1->TTL("tSetKey1", &ttl);
//    s = n1->Get("tSetKey1", &res);
//    log_info("Test Get OK return %s, result tSetKey1 = %s, ttl=%ld", s.ToString().c_str(), res.c_str(), ttl);
//
//    s = n1->TTL("tSetKey2", &ttl);
//    s = n1->Get("tSetKey2", &res);
//    log_info("Test Get OK return %s, result tSetKey2 = %s, ttl=%ld", s.ToString().c_str(), res.c_str(), ttl);
//
//    s = n1->TTL("tSetKey4", &ttl);
//    s = n1->Get("tSetKey4", &res);
//    log_info("Test Get OK return %s, result tSetKey4 = %s, ttl=%ld", s.ToString().c_str(), res.c_str(), ttl);
//
//    fvs.clear();
//    s = n1->HGetall("tHSetKey", fvs);
//    log_info("Test HGetall OK return %s", s.ToString().c_str());
//    for (fv_iter = fvs.begin(); fv_iter != fvs.end(); fv_iter++) {
//        log_info("Test HGetall, field: %s, val: %s", fv_iter->field.c_str(), fv_iter->val.c_str());
//    }
//    log_info("");
//
//    /*
//     *  Test LLen
//     */
//    log_info("======Test LLen======");
//    n1->LLen("tLPushKey", &llen);
//    log_info("Test LLen return %ld, expect 6", llen);
//
//    for (int i = 0; i < llen; i++) {
//        s = n1->LPop("tLPushKey", &res);
//        log_info("Test LPop OK return %s, res = %s", s.ToString().c_str(), res.c_str());
//    }
//    log_info("");

    /********
     * dump specify
     ********/
    sleep(2);
    log_info("================");
    log_info("======Test Dump specify ======");
    snapshots.clear();

    s = n->Set("tSetKey1", "tSetVal1");
    s = n->HSet("tHSetKey", "member1", "value1");

    int64_t ret;
    s = n->Expire("tHSetKeyTTL", 30, &ret);

    s = n->BGSaveGetSnapshot(snapshots);
    log_info("Get Snapshots return %s, expect size=5, result size=%lu", s.ToString().c_str(), snapshots.size());
    //for (int i = 0; i < snapshots.size(); i++) {
    //    printf ("%d: seqnumber=%d\n", i, snapshots[i]->GetSequenceNumber());
    //}

    sleep(2);
    s = n->Set("tSetKey1", "tSetVal1AfterSnapshot2");
    s = n->LPush("tLPushKey", "tLPushVal7AfterSnapshot2", &llen);
    s = n->HSet("tHSetKey", "member1", "value1AfterSnapshot2");
    s = n->ZAdd("key", 100.0, "zsetMemberAfterSnapshot", &za_res);
    s = n->SAdd("key", "memberAfterSnapshot", &sadd_res);

    s = n->BGSave(snapshots, "./dumpspecify");

#if 0
    Snapshot *snapshot;
    //const rocksdb::Snapshot *snapshot;

    /**** kv ******/
    s = n->Set("tSetKey1", "tSetVal1");

    s = n->BGSaveGetSpecifySnapshot(KV_DB, snapshot);
    log_info("Get SpecifySnapshot return %s", s.ToString().c_str());
    s = n->Set("tSetKey1", "tSetVal1AfterSnapshot2");
    s = n->BGSaveSpecify(KV_DB, snapshot, "./dumpspecify");

    /**** hash ****/
    s = n->HSet("tHSetKey", "member1", "value1");

    s = n->BGSaveGetSpecifySnapshot(HASH_DB, snapshot);
    log_info("Get hash SpecifySnapshot return %s", s.ToString().c_str());
    s = n->HSet("tHSetKey", "member1", "value1AfterSpecifySnapshot2");
    s = n->BGSaveSpecify(HASH_DB, snapshot, "./dumpspecify");

    /**** list ****/
    s = n->BGSaveGetSpecifySnapshot(LIST_DB, snapshot);
    log_info("Get list SpecifySnapshot return %s", s.ToString().c_str());
    s = n->LPush("tLPushKey", "tLPushVal7AfterSnapshot2", &llen);
    s = n->BGSaveSpecify(LIST_DB, snapshot, "./dumpspecify");
#endif

    Nemo *n2 = new Nemo("./dumpspecify/", options);

    res = "";
    s = n2->TTL("tSetKey1", &ttl);
    s = n2->Get("tSetKey1", &res);
    log_info("Test Get OK return %s, result tSetKey1 = %s, ttl=%ld", s.ToString().c_str(), res.c_str(), ttl);

    s = n2->TTL("tSetKey2", &ttl);
    s = n2->Get("tSetKey2", &res);
    log_info("Test Get OK return %s, result tSetKey2 = %s, ttl=%ld", s.ToString().c_str(), res.c_str(), ttl);

    s = n2->TTL("tSetKey4", &ttl);
    s = n2->Get("tSetKey4", &res);
    log_info("Test Get OK return %s, result tSetKey4 = %s, ttl=%ld", s.ToString().c_str(), res.c_str(), ttl);

    log_info("======Test Hash======");
    fvs.clear();
    s = n2->TTL("tHSetKey", &ttl);
    s = n2->HGetall("tHSetKey", fvs);
    log_info("Test HGetall tHSetKey OK return %s, ttl is %ld, expect ttl=-1", s.ToString().c_str(), ttl);
    for (fv_iter = fvs.begin(); fv_iter != fvs.end(); fv_iter++) {
        log_info("Test HGetall, field: %s, val: %s", fv_iter->field.c_str(), fv_iter->val.c_str());
    }
    log_info("");

    fvs.clear();
    s = n2->TTL("tHSetKeyTTL", &ttl);
    s = n2->HGetall("tHSetKeyTTL", fvs);
    log_info("Test HGetall tHSetKeyTTL OK return %s, ttl is %ld, expect a valid ttl", s.ToString().c_str(), ttl);
    for (fv_iter = fvs.begin(); fv_iter != fvs.end(); fv_iter++) {
        log_info("Test HGetall, field: %s, val: %s", fv_iter->field.c_str(), fv_iter->val.c_str());
    }
    log_info("");

    /*
     *  Test LLen
     */
    log_info("======Test LLen======");
    n2->LLen("tLPushKey", &llen);
    log_info("Test LLen return %ld, expect 6", llen);

    for (int i = 0; i < llen; i++) {
        s = n2->LPop("tLPushKey", &res);
        log_info("Test LPop OK return %s, res = %s", s.ToString().c_str(), res.c_str());
    }
    log_info("");
    
    /*
     * zset
     */
    std::vector<SM> sms;
    std::vector<SM>::iterator it_sm;
    log_info("======Test ZSet======");
    sms.clear();
    log_info("New ZCard, return %ld", n->ZCard("key"));
    s = n2->ZRange("key", 0, -1, sms);
    log_info("ZRange key return %s, expect 2 [member1, member2]", s.ToString().c_str());
    for (it_sm = sms.begin(); it_sm != sms.end(); it_sm++) {
        log_info("Test ZRange score: %lf, member: %s", it_sm->score, it_sm->member.c_str());
    }
    log_info("");

    /*
     *  Test SMembers
     */
    log_info("======Test Set======");
    log_info("=====Test SMembers======");
    std::vector<std::string> values;
    values.clear();
    s = n2->SMembers("key", values);
    log_info("Test SMembers return %s, expect [member1, member2]", s.ToString().c_str());
    std::vector<std::string>::iterator smit;
    for (smit = values.begin(); smit != values.end(); smit++) {
        log_info("Test SMembers member: %s", smit->c_str());
    }
    log_info("");

    delete n;
//    delete n1;
    delete n2;
    return 0;
}

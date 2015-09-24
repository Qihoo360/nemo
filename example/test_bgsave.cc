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
    Nemo *n = new Nemo("./tmp/", options); 
    Status s;

    s = n->Set("tSetKey1", "tSetVal1");
    s = n->Set("tSetKey2", "tSetVal2");
    s = n->Set("tSetKey3", "tSetVal3");

    s = n->Set("tSetKey4", "tSetVal4", 300);
    sleep(2);

    s = n->HSet("tHSetKey", "member1", "value1");
    s = n->HSet("tHSetKey", "member2", "value2");
    s = n->HSet("tHSetKey", "member3", "value3");
    

    int64_t llen;
    s = n->LPush("tLPushKey", "tLPushVal1", &llen);
    s = n->LPush("tLPushKey", "tLPushVal2", &llen);
    s = n->LPush("tLPushKey", "tLPushVal3", &llen);
    s = n->LPush("tLPushKey", "tLPushVal4", &llen);
    s = n->LPush("tLPushKey", "tLPushVal5", &llen);
    s = n->LPush("tLPushKey", "tLPushVal6", &llen);

    /*
     *  Test LLen
     */
    n->LLen("tLPushKey", &llen);
    log_info("Origin LLen return %ld", llen);
    log_info("");


    s = n->BGSave();

    Nemo *n1 = new Nemo("./dump/", options); 
     
    std::string res = "";
    int64_t ttl;
    s = n1->TTL("tSetKey1", &ttl);
    s = n1->Get("tSetKey1", &res);
    log_info("Test Get OK return %s, result tSetKey1 = %s, ttl=%ld", s.ToString().c_str(), res.c_str(), ttl);

    s = n1->TTL("tSetKey2", &ttl);
    s = n1->Get("tSetKey2", &res);
    log_info("Test Get OK return %s, result tSetKey2 = %s, ttl=%ld", s.ToString().c_str(), res.c_str(), ttl);

    s = n1->TTL("tSetKey4", &ttl);
    s = n1->Get("tSetKey4", &res);
    log_info("Test Get OK return %s, result tSetKey4 = %s, ttl=%ld", s.ToString().c_str(), res.c_str(), ttl);

    std::vector<FV> fvs;
    fvs.clear();
    s = n1->HGetall("tHSetKey", fvs);
    log_info("Test HGetall OK return %s", s.ToString().c_str());
    std::vector<FV>::iterator fv_iter;
    for (fv_iter = fvs.begin(); fv_iter != fvs.end(); fv_iter++) {
        log_info("Test HGetall, field: %s, val: %s", fv_iter->field.c_str(), fv_iter->val.c_str());
    }
    log_info("");

    /*
     *  Test LLen
     */
    log_info("======Test LLen======");
    n1->LLen("tLPushKey", &llen);
    log_info("Test LLen return %ld, expect 6", llen);

    for (int i = 0; i < llen; i++) {
        s = n1->LPop("tLPushKey", &res);
        log_info("Test LPop OK return %s, res = %s", s.ToString().c_str(), res.c_str());
    }
    log_info("");

    delete n;
    delete n1;
    return 0;
}

#include "nemo.h"
#include "xdebug.h"

#include <vector>
#include <string>

#include "utilities/util.h"
#include "nemo_zset.h"

using namespace nemo;

int main()
{
    std::string a = "123abc";
    int64_t ival;
    double dval;
    if (StrToInt64(a.data(), a.size(), &ival)) {
        printf ("ival is %lld\n", ival);
    } else {
        printf ("StrToInt64 failed (%s).\n", a.c_str());
    }

    if (StrToDouble(a.data(), a.size(), &dval)) {
        printf ("dval is %lld\n", dval);
    } else {
        printf ("StrToDouble failed (%s).\n", a.c_str());
    }

    printf ("EncodeScore(1000.123456) %lld\n", EncodeScore(1000.123456));
    printf ("DecodeScore(1000100012346) %f\n", DecodeScore(1000100012346));

    std::string ret = EncodeZScoreKey("key", "member", 1000.123456);
    printf ("EncodeZscoreKey() -> size=%d (%s)\n", ret.size(), ret.c_str());
    std::string key;
    std::string member;
    double score;
    DecodeZScoreKey(ret, &key, &member, &score);
    printf ("DecodeZscoreKey(%s) key=(%s) member=(%s) score=(%f)\n", ret.c_str(), key.c_str(), member.c_str(), score);

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
     *************************************************ZSet**************************************************
     */

    /*
     *  Test ZAdd
     */
    log_info("======Test ZAdd======");
    s = n->ZAdd("tZAddKey", 0, "tZAddMem0");
    s = n->ZAdd("tZAddKey", 1, "tZAddMem1");
    s = n->ZAdd("tZAddKey", 2.3, "tZAddMem1_2");
    s = n->ZAdd("tZAddKey", 2, "tZAddMem2");
    s = n->ZAdd("tZAddKey", 3.3, "tZAddMem2_2");
    s = n->ZAdd("tZAddKey", 3, "tZAddMem3");
    s = n->ZAdd("tZAddKey", 7, "tZAddMem7");
    log_info("Test ZAdd OK return %s", s.ToString().c_str());
    log_info("");

    /*
     *  Test ZCard
     */
    log_info("======Test ZCard======");
    log_info("Test ZCard, return %ld", n->ZCard("tZAddKey"));
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

    return 0;
}

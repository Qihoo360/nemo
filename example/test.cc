#include "nemo.h"
#include "xdebug.h"

#include <vector>
#include <string>

#include "utilities/util.h"

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

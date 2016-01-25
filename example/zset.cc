#include <iostream>
#include <vector>
#include <string>
#include <ctime>
#include <inttypes.h>
#include <limits>

#include "nemo.h"
#include "xdebug.h"
#include "../src/nemo_zset.h"

using namespace nemo;

int main()
{
//    double k = -0.1; 
//    printf ("EncodeScore(1000.123456) %ld\n", EncodeScore(1000.123456));
//    printf ("DecodeScore(Encode(1000.123456) %f\n", DecodeScore(EncodeScore(1000.123456)));
//
//    printf ("EncodeScore(%lf) %ld\n",  -0.1, EncodeScore(-0.1));
//    printf ("DecodeScore(Encode(%lf)) %f\n", -0.1, DecodeScore(EncodeScore(-0.1)));
//
//    k = 0;
//    printf ("EncodeScore(%lf) %ld\n",  k, EncodeScore(k));
//    printf ("DecodeScore(Encode(%lf)) %f\n", k, DecodeScore(EncodeScore(k)));
//
//    k = -1;
//    printf ("EncodeScore(%lf) %ld\n",  k, EncodeScore(k));
//    printf ("DecodeScore(Encode(%lf)) %f\n", k, DecodeScore(EncodeScore(k)));

    nemo::Options options;
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
    std::vector<SM>::iterator it_sm;

    /*
     *  Test ZRank
     */
//    log_info("======Test ZRank======");
    int64_t rank;
    int64_t zadd_res;
//    s = n->ZAdd("zr", 1, "a", &zadd_res);
//    s = n->ZAdd("zr", 0.2, "b", &zadd_res);
//
//    s = n->ZRange("zr", 0, -1, sms);
//    for (it_sm = sms.begin(); it_sm != sms.end(); it_sm++) {
//        log_info("          score: %lf, member: %s", it_sm->score, it_sm->member.c_str());
//    }
//    log_info("");
//
//    s = n->ZRank("zr", "a", &rank);
//    log_info("Test ZRank (a) return %s, rank: %ld", s.ToString().c_str(), rank);
//    s = n->ZRank("zr", "b", &rank);
//    log_info("Test ZRank (b) return %s, rank: %ld", s.ToString().c_str(), rank);

    /*
     *  Test ZRemrangebyscore
     */
    log_info("======Test ZRemrangebyscore ======");
    s = n->ZAdd("zr", -1, "a", &zadd_res);
    s = n->ZAdd("zr", -0.1, "b", &zadd_res);
    s = n->ZAdd("zr", 0, "c", &zadd_res);
    s = n->ZAdd("zr", 1, "d", &zadd_res);
    s = n->ZAdd("zr", 2, "e", &zadd_res);


    int64_t count;
    s = n->ZRemrangebyscore("zr", 0, 0, &count);  
    log_info("Test ZRremrangebyscore (0, 0) return %s, count is %ld, expect 1", s.ToString().c_str(), count);

    log_info("    After ZRremrangebyscore (0, 0), zrange expect 4 [a, b, d, e]:");
    sms.clear();
    s = n->ZRange("zr", 0, -1, sms);
    for (it_sm = sms.begin(); it_sm != sms.end(); it_sm++) {
        log_info("          score: %lf, member: %s", it_sm->score, it_sm->member.c_str());
    }
    log_info("");


    s = n->ZRemrangebyscore("zr", -1, 0, &count);  
    log_info("Test ZRremrangebyscore (-1, 0) return %s, count is %ld, expect 2", s.ToString().c_str(), count);

    log_info("    After ZRremrangebyscore (-1, 0), zrange expect 2 [d, e]:");
    sms.clear();
    s = n->ZRange("zr", 0, -1, sms);
    for (it_sm = sms.begin(); it_sm != sms.end(); it_sm++) {
        log_info("          score: %lf, member: %s", it_sm->score, it_sm->member.c_str());
    }
    log_info("");

    double score;
    s = n->ZScore("zr", "a", &score);
    log_info( "zscore a return %s, score is %lf\n", s.ToString().c_str(), score);
    
    s = n->ZScore("zr", "b", &score);
    log_info( "zscore b return %s, score is %lf\n", s.ToString().c_str(), score);

    s = n->ZScore("zr", "d", &score);
    log_info( "zscore d return %s, score is %lf\n", s.ToString().c_str(), score);

    s = n->ZScore("zr", "e", &score);
    log_info( "zscore e return %s, score is %lf\n", s.ToString().c_str(), score);
    return 0;
}

#ifndef _CONFIG_H_
#define _CONFIG_H_
#include <vector>
#include <string>
#include <mutex>
#include <stdint.h>

#include "nemo.h"
#include "xdebug.h"

#define cmdLen 89

using namespace nemo;



typedef struct config{
    int numOfRequests;
    int numOfClients;
    int64_t totalLatency;
    int playLoad;
    int valueSize;
    int numOfKeys;
    int finishedRequests;
    int op;
    std::vector<int> ops;
    std::vector<int64_t> latency;
    std::vector<std::string> key;
    std::vector<std::string> value;
    std::vector<std::string> field;
    std::vector<nemo::KV> kv;
    std::vector<int64_t> ttl;
    std::mutex mtx;
}config;

void showReports(const config &conf);
void client_op(nemo::Nemo *np,config *conf);
void test_ops(nemo::Nemo *np,config *conf);
void reset(config &conf);

static std::string cmd[cmdLen]={"set","setttl","get","mset","mget","kmdel","incrby","decrby","incrybyfloat","getset",
	"append","setnx","setxx","msetnx","getrange","setrange","strlen","kscan","scan","keys",
	"bitset","bitget","bitcount","bitpos","bitop","setwithexpireat","hset","hget","hdel","hexists",
	"hkeys","hgetall","hlen","hmset","hmget","hsetnx","hstrlen","hscan","hvals","hincrby",
	"hincrbyfloat","lindex","llen","lpush","lpop","lpushx","lrange","lset","ltrim","rpush",
	"rpop","rpushx","rpoplpush","linsert","lrem","zadd","zcard","zcount","zscan","zincrby",
	"zrange","zunionstore","zinterstore","zrangebyscore","zrem","zrank","zrevrank","zscore","zremrangebylex","zremrangebyrank",
	"zremrangebyscore","sadd","srem","scard","sscan","smembers","sunionstore","sunion","sinterstore","sinter",
	"sdifferstore","sdiff","sismember","spop","srandmember","smove","pfadd","pfcount","pfmerge"
};


#endif

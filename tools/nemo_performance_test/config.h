#ifndef CONFIG_H
#define CONFIG_H
#include <vector>
#include <string>
#include <mutex>
#include <stdint.h>

#include "nemo.h"
#include "xdebug.h"

#define cmdLen 89

using namespace nemo;

static std::string cmd[cmdLen] = {"set","setttl","get","mset","mget","kmdel","incrby","decrby","incrybyfloat","getset",
  "append","setnx","setxx","msetnx","getrange","setrange","strlen","kscan","scan","keys",
  "bitset","bitget","bitcount","bitpos","bitop","setwithexpireat","hset","hget","hdel","hexists",
  "hkeys","hgetall","hlen","hmset","hmget","hsetnx","hstrlen","hscan","hvals","hincrby",
  "hincrbyfloat","lindex","llen","lpush","lpop","lpushx","lrange","lset","ltrim","rpush",
  "rpop","rpushx","rpoplpush","linsert","lrem","zadd","zcard","zcount","zscan","zincrby",
  "zrange","zunionstore","zinterstore","zrangebyscore","zrem","zrank","zrevrank","zscore","zremrangebylex","zremrangebyrank",
  "zremrangebyscore","sadd","srem","scard","sscan","smembers","sunionstore","sunion","sinterstore","sinter",
  "sdifferstore","sdiff","sismember","spop","srandmember","smove","pfadd","pfcount","pfmerge"
};

typedef struct Config {
  int32_t num_of_requests;
  int32_t num_of_clients;
  int64_t total_latency;
  int32_t key_size;
  int32_t value_size;
  int32_t num_of_keys;
  int32_t finished_requests;
  int32_t op;
  int32_t m;
  std::string folder;

  std::vector<int32_t> ops;
  std::vector<int64_t> latency;
  std::vector<std::string> key;
  std::vector<std::string> value;
  std::vector<std::string> field;
  std::vector<nemo::KV> kv;
  std::vector<int64_t> ttl;
} Config;

typedef struct Client {
  //the index of the client
  int32_t no; 
  int32_t requests;
  int32_t finished_requests;
  //the latency of every request sent by this client
  std::vector<int64_t> latency;  
  Client(int index, int n):
    no(index),
    requests(n),
    finished_requests(0) {
      latency.reserve(n);
    }
} Client;

void ShowReports(const Config &conf, std::string file_name);
void ClientOP(nemo::Nemo *np, Config *conf, Client *ct);
void TestOPs(nemo::Nemo *np, Config *conf);
void Reset(Config &conf);



#endif

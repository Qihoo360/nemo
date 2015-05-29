#include "nemo.h"
//#include "rocksdb/options.h"
#include "xdebug.h"

#include <vector>
#include <string>

//using namespace rocksdb;
using namespace nemo;

int main()
{
    rocksdb::Options opt;
    opt.create_if_missing = true;
    Nemo *n = new Nemo("./tmp/db", opt); 
    rocksdb::Status s;

    std::string res;
    std::vector<Kv> kvs;
    std::vector<Kvs> kvss;

    std::vector<std::string> keys;
    keys.push_back("key1");
    keys.push_back("ky2");
    keys.push_back("key3");

    s = n->MultiGet(keys, kvss);
    std::vector<Kvs>::iterator iter;
    for(iter = kvss.begin(); iter != kvss.end(); iter++) {
        log_info("MultiGet key: %s, val: %s status: %s", iter->key.c_str(), iter->val.c_str(), iter->status.ToString().c_str());
    }


    s = n->MultiDel(keys);

    log_info("MultiDel return %s", s.ToString().c_str());
    kvs.push_back({"key7", "val7"});
    kvs.push_back({"key8", "val8"});
    kvs.push_back({"key9", "val9"});
    s = n->Set("incr", "-12");
    s = n->Incr("incr", 8, res);
    log_info("Incr val: %s", res.c_str());
    s = n->Incr("incr", -2, res);
    log_info("Incr val: %s", res.c_str());
    s = n->MultiSet(kvs);
    s = n->Delete("eyfield1");
    s = n->Delete("noexist");
    s = n->Set("songzhao", "lucky");
    s = n->Set("songzhao1", "lucky1");
    s = n->Set("songzhao2", "lucky2");
    s = n->Get("songzhao", &res);
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
    return 0;
}

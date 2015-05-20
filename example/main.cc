#include "nemo.h"
//#include "rocksdb/options.h"
#include "xdebug.h"

#include <vector>
#include <string>

//using namespace rocksdb;
using namespace nemo;

int main()
{
    Options opt;
    opt.create_if_missing = true;
    Nemo *n = new Nemo("./tmp/db", opt); 
    Status s;
    std::string res;
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
        log_info("key: %s, value: %s\n", it->key.c_str(), it->val.c_str());
        num++;
    }
    log_info("num = %d", num);

    n->HSet("key", "field1", "heihei");
    n->HSet("key", "field2", "heihei2");
    std::string val;
    n->HGet("key", "field1", &val);

    log_info("val %s", val.c_str());

    std::vector<std::string> v;
    n->HKeys("key", &v);

    return 0;
}

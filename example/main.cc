#include "nemo.h"
#include "rocksdb/options.h"
#include "xdebug.h"

#include <vector>
#include <string>

using namespace rocksdb;
using namespace nemo;

int main()
{
    Options opt;
    opt.create_if_missing = true;
    Nemo *n = new Nemo("./tmp/db", opt); 
    n->HSet("key", "field1", "heihei");
    n->HSet("key", "field2", "heihei2");
    std::string val;
    n->HGet("key", "field1", &val);

    log_info("val %s", val.c_str());

    std::vector<std::string> v;
    n->HKeys("key", &v);

    return 0;
}

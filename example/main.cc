#include "nemo.h"
#include "rocksdb/options.h"

using namespace rocksdb;
using namespace nemo;

int main()
{
    Options opt;
    Nemo *n = new Nemo("/tmp/db", opt); 
    n->Hset("key", "field1", "heihei");
    return 0;
}

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
    Nemo *n = new Nemo("./tmp", options); 
    Status s;


    std::vector<uint64_t> nums;

    n->GetKeyNum(nums);

    for (int i = 0; i < nums.size(); i++) {
      printf (" i=%d %ld\n", i, nums[i]);
    }

    uint64_t num;
    n->GetSpecifyKeyNum("kv", num);
    printf ("kv key num: %lu\n", num);

    n->GetSpecifyKeyNum("hash", num);
    printf ("hash key num: %lu\n", num);

    n->GetSpecifyKeyNum("list", num);
    printf ("list key num: %lu\n", num);

    n->GetSpecifyKeyNum("zset", num);
    printf ("zset key num: %lu\n", num);

    n->GetSpecifyKeyNum("set", num);
    printf ("set key num: %lu\n", num);

    s = n->GetSpecifyKeyNum("invalid type", num);
    printf ("test invalid type:  ");
    if (!s.ok()) {
        printf ("SUCCESS, expect !ok\n");
    } else {
        printf ("FAILED, return ok, should failed\n");
    }
    delete n;
    return 0;
}

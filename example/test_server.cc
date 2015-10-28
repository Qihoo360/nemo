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

    delete n;
    return 0;
}

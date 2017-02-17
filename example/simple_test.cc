#include <iostream>
#include <vector>
#include <string>
#include <ctime>
#include <inttypes.h>
#include <limits>

#include "nemo.h"
#include "xdebug.h"

using namespace nemo;

Nemo *n;

int main() {
  nemo::Options options;
  options.target_file_size_base = 20 * 1024 * 1024;

  n = new Nemo("./tmp/", options); 

  std::string res;

  Status s = n->HSet("tHSetKey", "field1", "value1");
  s = n->HSet("tHSetKey", "field2", "value2");
  std::cout << "HSet return: " << s.ToString() << std::endl;

  int l = n->HLen("tHSetKey");
  std::cout << "HLen return: " << l << std::endl;
  delete n;

  return 0;
}

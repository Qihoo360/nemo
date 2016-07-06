#include <iostream>
#include <vector>
#include <string>
#include <ctime>
#include <inttypes.h>
#include <limits>
#include <sys/time.h>

#include "nemo.h"
#include "xdebug.h"

using namespace nemo;

uint64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

int main(int argc, char* argv[]) {
  if (argc < 3) {
    printf ("Usage:\n"
            "  ./meta_clean  ./db_path  compact_thread\n"
            "example: ./meta_clean  ./db  4\n");
    exit(-1);
  }

  int num = atoi(argv[2]);
  nemo::Options options;
  options.target_file_size_base = 20 * 1024 * 1024;
  options.target_file_size_base = 20 * 1024 * 1024;
  option.write_buffer_size = 256 * 1024 * 1024;

  Nemo *n = new Nemo(argv[1], options); 

  uint64_t st = NowMicros();

  printf ("Start:\n"
          "    timestamp:  %lu\n" 
          "    db_path:    %s\n"
          "    compact_thread: %d\n", st, argv[1], num);

  Status s = n->Compact(nemo::kALL, true);
  if (s.ok()) {
    printf ("Finish ok!\n");
  } else {
    printf ("Finish failed, %s\n", s.ToString().c_str());
  }

  uint64_t tot = NowMicros() - st;
  printf ("Time: %lu.%06lu s\n", tot / 1000000, tot % 1000000);

  delete n;
  return 0;
}

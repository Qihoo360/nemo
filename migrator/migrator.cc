#include <iostream>
#include <vector>
#include <string>
#include <ctime>
#include <sys/time.h>
#include <inttypes.h>

#include "nemo.h"
#include "xdebug.h"

using namespace nemo;

void usage() {
  printf ("Usage:\n"
          "\told2new_db -s source_dir -d destination_dir\n"
          "\told2new_db -s source_dir -d destination_dir -w write_buffer_size(default 200M)\n"
          );
}

int main(int argc, char *argv[])
{
  if (argc < 5) {
    usage();
    return 0;
  }

  char src_path[1024];
  char dst_path[1024];
  memset(src_path, 0, sizeof(src_path));
  memset(dst_path, 0, sizeof(dst_path));

  nemo::Options options;
  options.write_buffer_size = 200 * 1024 * 1024;

  char c;
  while (-1 != (c = getopt(argc, argv, "s:d:w:h"))) {
        switch (c) {
        case 's':
            snprintf(src_path, 1024, "%s", optarg);
            break;
        case 'd':
            snprintf(dst_path, 1024, "%s", optarg);
            break;
        case 'w':
            char * pEnd;
            options.write_buffer_size = strtol(optarg, &pEnd, 10);
        case 'h':
            usage();
            return 0;
        default:
            usage();
            break;
        }
  }

  if (strlen(src_path) <= 0 || strlen(dst_path) <= 0) {
    usage();
    return 0;
  }

  printf ("migrate: \n"
          "\tsource dir is %s\n"
          "\tdestination is %s\n"
          "\twrite_bufer_size %d\n", src_path, dst_path, options.write_buffer_size);

  Nemo *n = new Nemo(src_path, options); 
  nemo::Status s;

  nemo::Snapshots snapshots;
  s = n->BGSaveGetSnapshot(snapshots);
  if (!s.ok()) {
    log_err("get snapshots error!");
  }
  n->BGSave(snapshots, dst_path);

  delete n;
  return 0;
}

#include <iostream>
#include <vector>
#include <string>
#include <ctime>
#include <sys/time.h>
#include <inttypes.h>

#include "nemo.h"
#include "xdebug.h"

using namespace nemo;

int main(int argc, char *argv[])
{
  if (argc < 3) {
    log_err("Input argument num wrong");
  }
  char *srcDir = argv[1];
  char *dstDir = argv[2];
  nemo::Options options;
  Nemo *n = new Nemo(srcDir, options); 
  nemo::Status s;

  nemo::Snapshots snapshots;
  s = n->BGSaveGetSnapshot(snapshots);
  if (!s.ok()) {
    log_err("get snapshots error!");
  }
  n->BGSave(snapshots, dstDir);

  delete n;
  return 0;
}

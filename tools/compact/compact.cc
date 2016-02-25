#include <iostream>
#include <assert.h>
#include "xdebug.h"
#include <string>
#include "nemo.h"

using namespace std;

void Usage() {
  cout << "Usage: " << endl;
  cout << "./compact db_path type" << endl;
  cout << "type is one of: kv, hash, list, zset, set, all" << endl;
}

int main(int argc, char **argv)
{
  if (argc != 3) {
    Usage();
    log_err("not enough parameter");
  }
  std::string path(argv[1]);
  std::string db_type(argv[2]);

  // Create nemo handle
  nemo::Options option;

  log_info("Prepare DB...");
  nemo::Nemo* db = new nemo::Nemo(path, option);
  assert(db);
  log_info("Compact Begin");

  nemo::Status s;
  if (db_type == "all") {
    s = db->Compact();
  } else {
    s = db->CompactSpecify(db_type);
  }
  delete db;

  if (!s.ok()) {
    Usage();
    log_err("Compact Failed : %s", s.ToString().c_str());
  } else {
    log_info("Compact Finshed");
  }
  return 0;
}

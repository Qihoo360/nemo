// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#include <cstdio>
#include <string>
#include <thread>
#include <chrono>
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/utilities/db_ttl.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using namespace rocksdb;
using namespace std;

#define TNUM 10
#define RNUM 1000000000

std::string kDBPath = "./rocksdb_compact_example";

char cvalue[1024];
std::string value;

void Run(DB* db, int start, int count) {
  int i = 0;
  std::string key;
  Status s;
  while (i < count) {
    key = "key";
    key.append((char*)(&i), 4);
    s = db->Put(WriteOptions(), key, value);
    i++;
  }
  cout << std::this_thread::get_id() << " done, count " << i << endl;
}


int main() {
  memset(cvalue, 'a', 1024);
  value = std::string(cvalue);

//  DB* db;
  DBWithTTL *db;
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.create_if_missing = true;
  options.write_buffer_size = 268435456;
  options.max_manifest_file_size = 64*1024*1024;
  options.target_file_size_base = 20971520;
  options.max_background_flushes = 1;
  options.max_background_compactions = 2;

  // open DB
  Status s = DBWithTTL::Open(options, kDBPath, &db);
  assert(s.ok());

  std::thread threads[TNUM];

  for (int i = 0; i < TNUM; i++) {
    threads[i] = std::thread(Run, db, i*(RNUM/TNUM), (RNUM/TNUM));
    std::cout << "start thread " << i << std::endl;
  }

  getchar();
  std::cout << "Begin CompactRange" << std::endl;
  s = db->CompactRange(NULL, NULL);
  std::cout << "End CompactRange" << std::endl;

  for (auto &t : threads) {
    t.join();
  }

  std::cout << "Done" << std::endl;

  delete db;

  return 0;
}

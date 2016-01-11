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

void* ThreadMain1(void *arg) {
  Status s = n->HSet("tHSetKey", "field1", "value1");
  log_info("Test HSet field1 OK return %s", s.ToString().c_str());
  return NULL;
}

void* ThreadMain2(void *arg) {
  Status s = n->HSet("tHSetKey", "field2", "value2");
  log_info("Test HSet field2 OK return %s", s.ToString().c_str());
  return NULL;
}

void* ThreadMain3(void *arg) {
  Status s = n->HSet("tHSetKey", "field3", "value3");
  log_info("Test HSet field3 OK return %s", s.ToString().c_str());
  return NULL;
}

void* ThreadMain4(void *arg) {
  Status s = n->HSet("tHSetKey1", "field3", "value3");
  log_info("Test HSet field3 OK return %s", s.ToString().c_str());
  return NULL;
}

void* ThreadMain5(void *arg) {
  Status s = n->HSet("tHSetKey2", "field3", "value3");
  log_info("Test HSet field3 OK return %s", s.ToString().c_str());
  return NULL;
}

void* ThreadMain6(void *arg) {
  Status s = n->HSet("tHSetKey3", "field3", "value3");
  log_info("Test HSet field3 OK return %s", s.ToString().c_str());
  return NULL;
}

int main() {
  nemo::Options options;
  options.target_file_size_base = 20 * 1024 * 1024;

  n = new Nemo("./tmp/", options); 
  Status s;

  std::string res;

  std::vector<std::string> keys;
  std::vector<KV> kvs;
  std::vector<KVS> kvss;
  std::vector<SM> sms;

  /*
   *************************************************HASH**************************************************
   */
  std::vector<std::string> fields;
  std::vector<std::string> values;
  std::vector<FV> fvs;
  std::vector<FVS> fvss;

  log_info("======Test HSet Same key======");

  pthread_t tid[3];
  pthread_create(&tid[0], NULL, &ThreadMain1, NULL);
  pthread_create(&tid[1], NULL, &ThreadMain2, NULL);
  pthread_create(&tid[2], NULL, &ThreadMain3, NULL);

  for (int i = 0; i < 3; i++) {
      pthread_join(tid[i], 0); 
  }

  log_info("======Test HSet different key======");

  pthread_create(&tid[0], NULL, &ThreadMain4, NULL);
  pthread_create(&tid[1], NULL, &ThreadMain5, NULL);

  for (int i = 0; i < 2; i++) {
      pthread_join(tid[i], 0); 
  }

  log_info("======Test HSet different key with all the lru in use======");
 // pthread_create(&tid[0], NULL, &ThreadMain4, NULL);
 // pthread_create(&tid[1], NULL, &ThreadMain5, NULL);
 // pthread_create(&tid[2], NULL, &ThreadMain6, NULL);

 // for (int i = 0; i < 3; i++) {
 //     pthread_join(tid[i], 0); 
 // }

  /*
   *  Test HGetall
   */
  log_info("======Test HGetall======");
  fvs.clear();
  s = n->HGetall("tHSetKey", fvs);
  log_info("Test HGetall OK return %s", s.ToString().c_str());
  std::vector<FV>::iterator fv_iter;
  for (fv_iter = fvs.begin(); fv_iter != fvs.end(); fv_iter++) {
    log_info("Test HGetall, field: %s, val: %s", fv_iter->field.c_str(), fv_iter->val.c_str());
  }
  log_info("");

  HIterator *hit = n->HScan("tHSetKey", "", "", -1);
  if (hit == NULL) {
    log_info("HScan error!");
  }
  for (; hit->Valid(); hit->Next()) {
    log_info("HScan key: %s, field: %s, value: %s", hit->key().c_str(), hit->field().c_str(), hit->value().c_str());
  }
  log_info("");
  delete hit;

  delete n;

  return 0;
}

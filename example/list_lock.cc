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
  int64_t llen;
  Status s = n->LPush("listKey", "value1", &llen);
  log_info("Test LPush (key, value1) OK return %s, llen=%ld", s.ToString().c_str(), llen);
  return NULL;
}

void* ThreadMain2(void *arg) {
  int64_t llen;
  Status s = n->LPush("listKey", "value2", &llen);
  log_info("Test LPush (key, value2) OK return %s, llen=%ld", s.ToString().c_str(), llen);
  return NULL;
}

void* ThreadMain3(void *arg) {
  int64_t llen;
  Status s = n->LPush("listKey", "value3", &llen);
  log_info("Test LPush (key, value3) OK return %s, llen=%ld", s.ToString().c_str(), llen);
  return NULL;
}

void* ThreadMain4(void *arg) {
  int64_t llen;
  Status s = n->LPush("listKey", "value3", &llen);
  log_info("Test LPush (key, value3) OK return %s, llen=%ld", s.ToString().c_str(), llen);
  return NULL;
}

void* ThreadMain5(void *arg) {
  int64_t llen;
  Status s = n->LPush("listKey1", "value3", &llen);
  log_info("Test LPush (key1, value3) OK return %s, llen=%ld", s.ToString().c_str(), llen);
  return NULL;
}

void* ThreadMain6(void *arg) {
  int64_t llen;
  Status s = n->LPush("listKey2", "value3", &llen);
  log_info("Test LPush (key2, value3) OK return %s, llen=%ld", s.ToString().c_str(), llen);
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

  log_info("======Test LPush Same key======");

  pthread_t tid[3];
  pthread_create(&tid[0], NULL, &ThreadMain1, NULL);
  pthread_create(&tid[1], NULL, &ThreadMain2, NULL);
  pthread_create(&tid[2], NULL, &ThreadMain3, NULL);

  for (int i = 0; i < 3; i++) {
      pthread_join(tid[i], 0); 
  }

  log_info("======Test LPush different key======");

  pthread_create(&tid[0], NULL, &ThreadMain4, NULL);
  pthread_create(&tid[1], NULL, &ThreadMain5, NULL);

  for (int i = 0; i < 2; i++) {
      pthread_join(tid[i], 0); 
  }

  //log_info("======Test LPush different key with all the lru in use======");
 // pthread_create(&tid[0], NULL, &ThreadMain4, NULL);
 // pthread_create(&tid[1], NULL, &ThreadMain5, NULL);
 // pthread_create(&tid[2], NULL, &ThreadMain6, NULL);

 // for (int i = 0; i < 3; i++) {
 //     pthread_join(tid[i], 0); 
 // }

  log_info("");

  delete n;

  return 0;
}

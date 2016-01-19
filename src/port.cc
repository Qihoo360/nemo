//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "port.h"

#include <stdio.h>
#include <assert.h>
#include <errno.h>
#include <sys/time.h>
#include <string.h>
#include <cstdlib>

#include "xdebug.h"

namespace nemo {
namespace port {

static int PthreadCall(const char* label, int result) {
  if (result != 0 && result != ETIMEDOUT) {
    fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
    abort();
  }
  return result;
}

Mutex::Mutex() { PthreadCall("init mutex", pthread_mutex_init(&mu_, nullptr)); }

Mutex::~Mutex() { PthreadCall("destroy mutex", pthread_mutex_destroy(&mu_)); }

void Mutex::Lock() { PthreadCall("lock", pthread_mutex_lock(&mu_)); }

void Mutex::Unlock() { PthreadCall("unlock", pthread_mutex_unlock(&mu_)); }

RWMutex::RWMutex() { PthreadCall("init mutex", pthread_rwlock_init(&mu_, nullptr)); }

RWMutex::~RWMutex() { PthreadCall("destroy mutex", pthread_rwlock_destroy(&mu_)); }

void RWMutex::ReadLock() { PthreadCall("read lock", pthread_rwlock_rdlock(&mu_)); }

void RWMutex::WriteLock() { PthreadCall("write lock", pthread_rwlock_wrlock(&mu_)); }

void RWMutex::ReadUnlock() { PthreadCall("read unlock", pthread_rwlock_unlock(&mu_)); }

void RWMutex::WriteUnlock() { PthreadCall("write unlock", pthread_rwlock_unlock(&mu_)); }

#include <pthread.h>

RecordMutex::~RecordMutex() {
  std::unordered_map<std::string, Mutex *>::const_iterator it = records_.begin();
  for (; it != records_.end(); it++) {
    delete it->second;
  }
}


void RecordMutex::Lock(const std::string &key) {
  mutex_.Lock();
  std::unordered_map<std::string, Mutex *>::const_iterator it = records_.find(key);

  if (it != records_.end()) {
    //log_info ("tid=(%u) >Lock key=(%s) exist", pthread_self(), key.c_str());
    Mutex *mu = it->second;
    mutex_.Unlock();

    mu->Lock();
    //log_info ("tid=(%u) <Lock key=(%s) exist", pthread_self(), key.c_str());
  } else {
    //log_info ("tid=(%u) >Lock key=(%s) new", pthread_self(), key.c_str());

    Mutex *mu = new Mutex();

    records_.insert(std::make_pair(key, mu));
    mutex_.Unlock();


    mu->Lock();
    //log_info ("tid=(%u) <Lock key=(%s) new", pthread_self(), key.c_str());
  }
}

void RecordMutex::Unlock(const std::string &key) {
  mutex_.Lock();
  std::unordered_map<std::string, Mutex *>::const_iterator it = records_.find(key);
  
  //log_info ("tid=(%u) >Unlock key=(%s) new", pthread_self(), key.c_str());
  if (it != records_.end()) {
    Mutex *mu = it->second;
    mutex_.Unlock();

    mu->Unlock();
  }
  //log_info ("tid=(%u) <Unlock key=(%s) new", pthread_self(), key.c_str());
}

}  // namespace port
}  // namespace nemo

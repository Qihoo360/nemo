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


CondVar::CondVar(Mutex* mu)
    : mu_(mu) {
    PthreadCall("init cv", pthread_cond_init(&cv_, NULL));
}

CondVar::~CondVar() { PthreadCall("destroy cv", pthread_cond_destroy(&cv_)); }

void CondVar::Wait() {
  PthreadCall("wait", pthread_cond_wait(&cv_, &mu_->mu_));
}

void CondVar::Signal() {
  PthreadCall("signal", pthread_cond_signal(&cv_));
}

void CondVar::SignalAll() {
  PthreadCall("broadcast", pthread_cond_broadcast(&cv_));
}


RWMutex::RWMutex() { PthreadCall("init mutex", pthread_rwlock_init(&mu_, nullptr)); }

RWMutex::~RWMutex() { PthreadCall("destroy mutex", pthread_rwlock_destroy(&mu_)); }

void RWMutex::ReadLock() { PthreadCall("read lock", pthread_rwlock_rdlock(&mu_)); }

void RWMutex::WriteLock() { PthreadCall("write lock", pthread_rwlock_wrlock(&mu_)); }

void RWMutex::ReadUnlock() { PthreadCall("read unlock", pthread_rwlock_unlock(&mu_)); }

void RWMutex::WriteUnlock() { PthreadCall("write unlock", pthread_rwlock_unlock(&mu_)); }

RecordMutex::~RecordMutex() {
  mutex_.Lock();
  
  //log_info ("tid=(%u) ~RecordMutex map_size=%u, lru_size=%u", pthread_self(), records_.size(), lru_.size());
  std::unordered_map<std::string, data_type>::iterator it = records_.begin();
  for (; it != records_.end(); it++) {
    delete it->second.first;
  }
  mutex_.Unlock();
}


void RecordMutex::Lock(const std::string &key) {
  mutex_.Lock();
  
  std::unordered_map<std::string, data_type>::iterator it = records_.find(key);

  if (it != records_.end()) {
    //log_info ("tid=(%u) >Lock key=(%s) exist, map_size=%u, lru_size=%u", pthread_self(), key.c_str(), records_.size(), lru_.size());

    data_type &data = it->second;
    Mutex *record_mutex = data.first;
    // update the iterator
    lru_.splice(lru_.end(), lru_, data.second);
    mutex_.Unlock();

    record_mutex->Lock();
    //log_info ("tid=(%u) <Lock key=(%s) exist", pthread_self(), key.c_str());
  } else {
    if (records_.size() == capacity_) {
      evict();
    }
    //log_info ("tid=(%u) >Lock key=(%s) new, map_size=%u ++, lru_size=%u", pthread_self(), key.c_str(), records_.size(), lru_.size());

    Mutex *record_mutex = new Mutex();
    std::list<std::string>::iterator it = lru_.insert(lru_.end(), key);
    records_.insert(std::make_pair(key, std::make_pair(record_mutex, it)));
    mutex_.Unlock();

    record_mutex->Lock();
    //log_info ("tid=(%u) <Lock key=(%s) new", pthread_self(), key.c_str());
  }
}

void RecordMutex::Unlock(const std::string &key) {
  mutex_.Lock();

  std::unordered_map<std::string, data_type>::iterator it = records_.find(key);

  //log_info ("tid=(%u) >Unlock key=(%s) new, map_size=%u, lru_size=%u", pthread_self(), key.c_str(), records_.size(), lru_.size());
  if (it != records_.end()) {
    Mutex *record_mutex = it->second.first;
    record_mutex->Unlock();
  }

  mutex_.Unlock();
  //log_info ("tid=(%u) <Unlock key=(%s) new", pthread_self(), key.c_str());
}

void RecordMutex::evict() {
  assert(!lru_.empty());

  //log_info ("tid=(%u) evict map_size=%u --, lru_size=%u --", pthread_self(), records_.size(), lru_.size());
  // check
  std::unordered_map<std::string, data_type>::iterator it = records_.find(lru_.front());
  assert(it != records_.end());

  // should ensure the mutex isn't used
  records_.erase(it);
  delete it->second.first;
  lru_.pop_front();
}

}  // namespace port
}  // namespace nemo

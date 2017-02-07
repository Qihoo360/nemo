#include "nemo_hyperloglog.h"
#include "nemo_list.h"
#include "nemo_mutex.h"
#include "nemo_murmur3.h"

using namespace nemo;

#define HLL_HASH_SEED 313

HyperLogLog::HyperLogLog(uint8_t precision, std::string origin_register) {
  b_ = precision;
  m_ = 1 << precision;
  alpha_ = Alpha();
  register_ = new char[m_];
  for (uint32_t i = 0; i < m_; ++i)
    register_[i] = 0;
  if (origin_register != "") {
    for (uint32_t i = 0; i < m_; ++i) {
      register_[i] = origin_register[i];
    }
  }
}

HyperLogLog::~HyperLogLog() {
  delete [] register_;
}

std::string HyperLogLog::Add(const char * str, uint32_t len) {
  uint32_t hash;
  MurmurHash3_x86_32(str, len, HLL_HASH_SEED, (void*) &hash);
  int index = hash & ((1 << b_) - 1);
  uint8_t rank = Nclz((hash << b_), 32 - b_);
  if (rank > register_[index])
    register_[index] = rank;
  std::string result_str(m_, 0);
  for (uint32_t i = 0; i < m_; ++i) {
    result_str[i] = register_[i];
  }
  return result_str;
}

double HyperLogLog::Estimate() const {
  double estimate = FirstEstimate();
    
  if (estimate <= 2.5* m_) {
    uint32_t zeros = CountZero();
    if (zeros != 0) {
      estimate = m_ * log((double)m_ / zeros);
    }
  } else if (estimate > pow(2, 32) / 30.0) {
    estimate = log1p(estimate * -1 / pow(2, 32)) * pow(2, 32) * -1;
  }
  return estimate;
}

double HyperLogLog::FirstEstimate() const {
  double estimate, sum = 0.0;
  for (uint32_t i = 0; i < m_; i++)
    sum += 1.0 / (1 << register_[i]);
    
  estimate = alpha_ * m_ * m_ / sum;
  return estimate;
}

double HyperLogLog::Alpha() const {
  switch (m_) {
    case 16:
      return 0.673;
    case 32:
      return 0.697;
    case 64:
      return 0.709;
    default:
      return 0.7213/(1 + 1.079 / m_);
    }
}

int HyperLogLog::CountZero() const {
  int count = 0;
  for(uint32_t i = 0;i < m_; i++) {
    if (register_[i] == 0) {
      count++;
    }
  }
  return count;
}

std::string HyperLogLog::Merge(const HyperLogLog & hll) {
  if (m_ != hll.m_) {
    // TODO: ERROR "number of registers doesn't match"
  }
  for (uint32_t r = 0; r < m_; r++) {
    if (register_[r] < hll.register_[r]) {
      register_[r] |= hll.register_[r];
    }
  }
    
  std::string result_str(m_, 0);
  for (uint32_t i = 0; i < m_; ++i) {
    result_str[i] = register_[i];
  }
  return result_str;
}

//::__builtin_clz(x): 返回左起第一个‘1’之前0的个数
uint8_t HyperLogLog::Nclz(uint32_t x, int b) {
  return (uint8_t)std::min(b, ::__builtin_clz(x)) + 1;
}

Status Nemo::PfAdd(const std::string &key, const std::vector<std::string> &values, bool & update) {
  if (values.size() >= KEY_MAX_LENGTH || values.size() <= 0) {
    return Status::InvalidArgument("Invalid value length");
  }

  Status s;
  std::string val, str_register="", result;
  s = Get(key, &val);
  if (s.ok()) {
    str_register = val;
  } else if (s.IsNotFound()) {
    str_register = "";
  }
  HyperLogLog log(10, str_register);
  int previous = int(log.Estimate());
  for (int i = 0; i < values.size(); ++i) {
    result = log.Add(values[i].data(), values[i].size());
  }
  HyperLogLog update_log(10,result);
  int now= int(update_log.Estimate());
  if (previous != now) {
    update = true;
  }
  s = kv_db_->Put(rocksdb::WriteOptions(), key, result);
  return s;
}
   
Status Nemo::PfCount(const std::vector<std::string> &keys, int & result) {
  if (keys.size() >= KEY_MAX_LENGTH || keys.size() <= 0) {
    return Status::InvalidArgument("Invalid key length");
  }

  Status s, ok;
  std::string value, str_register;
  s = Get(keys[0], &value);
  if (s.ok()) {
    str_register = std::string(value.data(), value.size());
  } else if (s.IsNotFound()) {
    str_register = "";
  }
	
  HyperLogLog first_log(10, str_register);
  for (int i = 1; i < keys.size(); ++i) {
    std::string value, str_register;
    s = Get(keys[i], &value);
    if (s.ok()) {
      str_register = value;
    } else if (s.IsNotFound()) {
      continue;
    }
    HyperLogLog log(10, str_register);
    first_log.Merge(log);
  }
  result = int(first_log.Estimate());
  return ok;
}

Status Nemo::PfMerge(const std::vector<std::string> &keys) {
  if (keys.size() >= KEY_MAX_LENGTH || keys.size() <= 0) {
    return Status::InvalidArgument("Invalid key length");
  }

  Status s;
  std::string value, str_register, result;
  s = Get(keys[0], &value);
  if (s.ok()) {
    str_register = std::string(value.data(), value.size());
  } else if (s.IsNotFound()) {
    str_register = "";
  }

  result = str_register;
  HyperLogLog first_log(10, str_register);
  for (int i = 1; i < keys.size(); ++i) {
    std::string value, str_register;
    s = Get(keys[i], &value);
    if (s.ok()) {
      str_register = std::string(value.data(), value.size());
    } else if (s.IsNotFound()) {
      continue;
    }
    HyperLogLog log(10, str_register);
    result = first_log.Merge(log); 
  }
  s = kv_db_->Put(rocksdb::WriteOptions(), keys[0], result);
  return s;
}


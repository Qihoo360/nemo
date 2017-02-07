#ifndef NEMO_INCLUDE_NEMO_HYPERLOGLOG_H
#define NEMO_INCLUDE_NEMO_HYPERLOGLOG_H

#include <cstdint>
#include <cmath>
#include <vector>
#include <map>
#include <algorithm>

#include "nemo.h"
#include "nemo_const.h"
#include "decoder.h"

namespace nemo {

struct HyperLogLog {
 public:
  HyperLogLog(uint8_t precision, std::string origin_register);
  ~HyperLogLog();
    
  double Estimate() const;
  double FirstEstimate() const;
  int CountZero() const;
  double Alpha() const;
  uint8_t Nclz(uint32_t x, int b);
    
  std::string Add(const char * str, uint32_t len);
  std::string Merge(const HyperLogLog & hll);
    
 protected:
  uint32_t m_;                    // register bit width
  uint8_t b_;                     // register size
  double alpha_;
  char * register_;               // registers
};

}
#endif

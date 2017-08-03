#ifndef TIME_KEEPER_H
#define TIME_KEEPER_H
#include <stdio.h>
#include <sys/time.h>
#include <stdint.h>

class TimeKeeper{
  public:
    TimeKeeper() {
      gettimeofday(&time_start,NULL); 
      time_end = time_start;
    }

    ~TimeKeeper() { }
    void Restart() {
      gettimeofday(&time_start,NULL);
    }
 
    //latency in millisecond
    int64_t DelayMs() {    
      gettimeofday(&time_end,NULL); 
      int64_t total_micros = (time_end.tv_sec - time_start.tv_sec) * 1000 + (time_end.tv_usec - time_start.tv_usec) / 1000; 
      return total_micros; 
    }

    //latency in second
    int64_t DelaySe() {      
      gettimeofday(&time_end,NULL); 
      int64_t total_s = time_end.tv_sec - time_start.tv_sec; 
      return total_s;
    } 

    //latency in microsecond
    int64_t DelayUs() {
      gettimeofday(&time_end,NULL);
      int64_t total_us = (time_end.tv_sec-time_start.tv_sec)*1000000+( time_end.tv_usec - time_start.tv_usec); 
      return total_us;
    }
    
  private:

    struct timeval time_start;
    struct timeval time_end;

};

#endif

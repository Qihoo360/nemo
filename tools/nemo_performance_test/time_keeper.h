#ifndef TIME_KEEPER_H_
#define TIME_KEEPER_H_
#include <stdio.h>
#include <sys/time.h>
#include <stdint.h>

class time_keeper{
public:
    time_keeper() {gettimeofday(&time_start,NULL); time_end=time_start;}
    ~time_keeper() { }
    void restart() {gettimeofday(&time_start,NULL);}
    int64_t delay_ms() { gettimeofday(&time_end,NULL); int64_t total_micros=(time_end.tv_sec-time_start.tv_sec)*1000+(time_end.tv_usec-time_start.tv_usec)/1000; return total_micros; }
    int64_t delay_s(){gettimeofday(&time_end,NULL); int64_t total_s=time_end.tv_sec-time_start.tv_sec; return total_s; } 
    int64_t delay_us(){gettimeofday(&time_end,NULL);int64_t total_us=(time_end.tv_sec-time_start.tv_sec)*1000000+(time_end.tv_usec-time_start.tv_usec); return total_us;}
private:
    struct timeval time_start;
    struct timeval time_end;
};

#endif

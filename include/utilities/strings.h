#ifndef UTIL_STRING_H
#define UTIL_STRING_H

#include <inttypes.h>
#include <unistd.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <limits.h>
#include <errno.h>
#include <string.h>
#include <math.h>
#include <fcntl.h>
#include <assert.h>
#include <signal.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string>
#include <algorithm>

namespace nemo {

inline int IsEmptyStr(const char *str){
	const char *p = str;
	while(*p && isspace(*p)){
		p++;
	}
	return *p == '\0';
}

inline char *Ltrim(const char *str){
	const char *p = str;
	while(*p && isspace(*p)){
		p++;
	}
	return (char *)p;
}

inline char *Rtrim(char *str){
	char *p;
	p = str + strlen(str) - 1;
	while(p >= str && isspace(*p)){
		p--;
	}
	*(++p) = '\0';
	return p;
}

inline char *Trim(char *str){
	char *p;
	p = Ltrim(str);
	Rtrim(p);
	return p;
}

inline void StrToLower(std::string *str){
	std::transform(str->begin(), str->end(), str->begin(), ::tolower);
}

inline void StrToUpper(std::string *str){
	std::transform(str->begin(), str->end(), str->begin(), ::toupper);
}

inline int StrToInt(const char *p, int size){
	return atoi(std::string(p, size).c_str());
}

inline int StrToInt(const std::string &str){
	return atoi(str.c_str());
}

inline std::string IntToStr(int v){
	char buf[21] = {0};
	snprintf(buf, sizeof(buf), "%d", v);
	return std::string(buf);
}

inline int64_t StrToInt64(const std::string &str){
	return (int64_t)atoll(str.c_str());
}

inline int64_t StrToInt64(const char *p, int size){
	return (int64_t)atoll(std::string(p, size).c_str());
}

inline std::string Int64ToStr(int64_t v){
	char buf[21] = {0};
	snprintf(buf, sizeof(buf), "%" PRId64 "", v);
	return std::string(buf);
}

inline uint64_t StrToUint64(const char *p, int size){
	return (uint64_t)strtoull(std::string(p, size).c_str(), (char **)NULL, 10);
}

inline std::string Uint64ToStr(uint64_t v){
	char buf[21] = {0};
	snprintf(buf, sizeof(buf), "%" PRIu64 "", v);
	return std::string(buf);
}

inline double StrToDouble(const char *p, int size){
	return atof(std::string(p, size).c_str());
}

inline std::string DoubleToStr(double v){
	char buf[21] = {0};
	if(v - floor(v) == 0){
		snprintf(buf, sizeof(buf), "%.0f", v);
	}else{
		snprintf(buf, sizeof(buf), "%f", v);
	}
	return std::string(buf);
}

}


#endif

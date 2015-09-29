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

#include "util.h"

namespace nemo {

int StrToUint64(const char *s, size_t slen, uint64_t *value) {
    const char *p = s;
    size_t plen = 0;
    uint64_t v;

    if (plen == slen)
        return 0;

    /* Special case: first and only digit is 0. */
    if (slen == 1 && p[0] == '0') {
        if (value != NULL) *value = 0;
        return 1;
    }

    while (plen < slen && p[0] == '0') {
        p++; plen++;
    }

    if (plen == slen) {
        if (value != NULL) *value = 0;
        return 1;
    }

    /* First digit should be 1-9, otherwise the string should just be 0. */
    if (p[0] >= '1' && p[0] <= '9') {
        v = p[0]-'0';
        p++; plen++;
    } else if (p[0] == '0' && slen == 1) {
        *value = 0;
        return 1;
    } else {
        return 0;
    }

    while (plen < slen && p[0] >= '0' && p[0] <= '9') {
        if (v > (ULLONG_MAX / 10)) /* Overflow. */
            return 0;
        v *= 10;

        if (v > (ULLONG_MAX - (p[0]-'0'))) /* Overflow. */
            return 0;
        v += p[0]-'0';

        p++; plen++;
    }

    /* Return if not all bytes were used. */
    if (plen < slen)
        return 0;

    if (v > LLONG_MAX) /* Overflow. */
      return 0;
    if (value != NULL) *value = v;

    return 1;
}

int StrToInt64(const char *s, size_t slen, int64_t *value) {
    const char *p = s;
    size_t plen = 0;
    int negative = 0;
    uint64_t v;

    if (plen == slen)
        return 0;

    /* Special case: first and only digit is 0. */
    if (slen == 1 && p[0] == '0') {
        if (value != NULL) *value = 0;
        return 1;
    }

    if (p[0] == '-') {
        negative = 1;
        p++; plen++;

        /* Abort on only a negative sign. */
        if (plen == slen)
            return 0;
    }

    if (!StrToUint64(p, slen - plen, &v))
        return 0;

    if (negative) {
        if (v > ((unsigned long long)(-(LLONG_MIN+1))+1)) /* Overflow. */
            return 0;
        if (value != NULL) *value = -v;
    } else {
        if (v > LLONG_MAX) /* Overflow. */
            return 0;
        if (value != NULL) *value = v;
    }
    return 1;
}

int StrToInt32(const char *s, size_t slen, int32_t *val) {
    int64_t llval;

    if (!StrToInt64(s, slen, &llval))
        return 0;

    if (llval < LONG_MIN || llval > LONG_MAX)
        return 0;

    *val = (int32_t)llval;
    return 1;
}

int StrToUint32(const char *s, size_t slen, uint32_t *val) {
    uint64_t llval;

    if (!StrToUint64(s, slen, &llval))
        return 0;

    if (llval > ULONG_MAX)
        return 0;

    *val = (uint32_t)llval;
    return 1;
}

int StrToDouble(const char *s, size_t slen, double *dval) {
    char *pEnd;
    double d = strtod(s, &pEnd);
    if (pEnd != s + slen) 
        return 0;
    
    if (dval != NULL) *dval = d;
    return 1;
}

}

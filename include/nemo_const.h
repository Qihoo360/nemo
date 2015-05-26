#ifndef NEMO_CONST_H_
#define NEMO_CONST_H_

static const int NEMO_SCORE_WIDTH       = 9;
static const int NOMO_KEY_LEN_MAX       = 255;

class DataType{
public:
    static const char SYNCLOG   = 1;
    static const char KV        = 'k';
    static const char HASH      = 'h'; // hashmap(sorted by key)
    static const char HSIZE     = 'H';
    static const char ZSET      = 's'; // key => score
    static const char ZSCORE    = 'z'; // key|score => ""
    static const char ZSIZE     = 'Z';
    static const char QUEUE     = 'q';
    static const char QSIZE     = 'Q';
    static const char MIN_PREFIX = HASH;
    static const char MAX_PREFIX = ZSET;
};

#endif

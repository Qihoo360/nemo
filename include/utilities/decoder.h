#ifndef NEMO_INCLUDE_DECODER_H
#define NEMO_INCLUDE_DECODER_H

#include <string>

class Decoder{
private:
    const char *p;
    int size;
    Decoder(){}
public:
    Decoder(const char *p, int size){
        this->p = p;
        this->size = size;
    }
    int skip(int n){
        if(size < n){
            return -1;
        }
        p += n;
        size -= n;
        return n;
    }
    int read_int64(int64_t *ret){
        if(size_t(size) < sizeof(int64_t)){
            return -1;
        }
        if(ret){
            *ret = *(int64_t *)p;
        }
        p += sizeof(int64_t);
        size -= sizeof(int64_t);
        return sizeof(int64_t);
    }
    int read_uint64(uint64_t *ret){
        if(size_t(size) < sizeof(uint64_t)){
            return -1;
        }
        if(ret){
            *ret = *(uint64_t *)p;
        }
        p += sizeof(uint64_t);
        size -= sizeof(uint64_t);
        return sizeof(uint64_t);
    }
    int read_data(std::string *ret){
        int n = size;
        if(ret){
            ret->assign(p, size);
        }
        p += size;
        size = 0;
        return n;
    }
    int read_8_data(std::string *ret=NULL){
        if(size < 1){
            return -1;
        }
        int len = (uint8_t)p[0];
        p += 1;
        size -= 1;
        if(size < len){
            return -1;
        }
        if(ret){
            ret->assign(p, len);
        }
        p += len;
        size -= len;
        return 1 + len;
    }
};
#endif

#ifndef NEMO_INCLUDE_DECODER_H
#define NEMO_INCLUDE_DECODER_H

#include <string>

class Decoder {
public:
    Decoder(const char *p, int32_t size)
        : ptr_(p),
        size_(size) {}

    int32_t Skip(int32_t n) {
        if (size_ < n) {
            return -1;
        }
        ptr_ += n;
        size_ -=n;
        return n;
    }

    int32_t ReadData(std::string *res) {
        int32_t n = size_;
        if (res) {
            res->assign(ptr_, size_);
        }
        ptr_ += size_;
        size_ = 0;
        return n;
    }

    int32_t ReadLenData(std::string *res = NULL) {
        if (size_ < 1) {
            return -1;
        }
        int32_t len = (uint16_t)ptr_[0];
        ptr_ += 1;
        size_ -= 1;
        if (size_ < len) {
            return -1;
        }
        if (res) {
            res->assign(ptr_, len);
        }
        ptr_ += len;
        size_ -= len;
        return len + 1;
    }
private:
    const char *ptr_;
    int32_t size_;
    //No Copying allowed
    Decoder(const Decoder&);
    void operator=(const Decoder&);
};

#endif

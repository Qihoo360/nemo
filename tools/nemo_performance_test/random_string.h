#ifndef RANDOM_STRING_H
#define RANDOM_STRING_H
#include <cmath>
#include <string>
#include <sys/time.h>
#include <stdint.h>
#include <vector>

#include "nemo_const.h"

class RandomString {
  public:
    RandomString() { 
      Usrand();
    }
    ~RandomString() {}

    std::string GetRandomChars(const int length) {
      //Usrand();
      std::string randomStr;
      for (int index = 0; index < length; index++) {
        randomStr += charSet[random() % charSetLen_];
      }
      return randomStr;
    }

    std::string GetRandomChars(const int min,const int max) {
      //Usrand();
      int len=min + random() % (max - min + 1);
      return GetRandomChars(len);
    }

    int32_t GetRandomInt() {
      // Usrand();
      return random();
    }

    std:: string GetRandomBytes(const int length) {
      //Usrand();
      std::string randomStr;
      for (int index = 0; index < length; index++)
      {
        randomStr += (char)(random() % 256);
      }
      return randomStr;
    }

    std::string GetRandomBytes(const int min,const int max) {
      //Usrand();
      int len = min + random() % ( max - min + 1);
      return GetRandomBytes(len);
    }

    std::string GetRandomKey() {
      return GetRandomBytes(minKeyLen_, maxKeyLen_);
    }

    std::string GetRandomVal() {
      return GetRandomBytes(minValLen_, maxValLen_);
    }

    std::string GetRandomField() {
      return GetRandomBytes(minFieldLen_, maxFieldLen_);
    }

    std::string GetRandomKey(int length) {
      return GetRandomBytes(length);
    }

    std::string GetRandomVal(int length) {
      return GetRandomBytes(length);
    }

    std::string GetRandomField(int length) {
      return GetRandomBytes(length);
    }

    std::vector<std::string>& GetRandomKeys(std::vector<std::string>&v_key, int key_size, int n) {
      for(int i=0;i<n;i++) {
        v_key.push_back(GetRandomKey(key_size));
      }
      return v_key;
    }

    std::vector<std::string>& GetRandomKeys(std::vector<std::string> &v_key,int n){
      for(int i=0;i<n;i++){
        v_key.push_back(GetRandomKey());
      }
      return v_key;
    }	


    std::vector<std::string>& GetRandomVals(std::vector<std::string> &v_value, int value_size, int n) {
      for(int i=0; i < n; i++) {
        v_value.push_back(GetRandomVal(value_size));
      }
      return v_value;
    }

    std::vector<std::string>& GetRandomVals(std::vector<std::string> &v_value, int n) {
      for(int i = 0;i < n; i++) {
        v_value.push_back(GetRandomVal());
      }
      return v_value;
    }

    std::vector<std::string>& GetRandomFields(std::vector<std::string> &v_field, int field_size, int n) {
      for(int i = 0; i < n; i++) {
        v_field.push_back(GetRandomField(field_size));
      }
      return v_field;
    }

    std::vector<std::string>& GetRandomFields(std::vector<std::string> &v_field, int n) {
      for(int i = 0 ;i < n ; i++) {
        v_field.push_back(GetRandomField());
      }
      return v_field;
    }

    void GetRandomInts(std::vector<int64_t> &v_int, int n) {
      // Usrand();
      for(int i = 0;i < n; i++) {
        v_int.push_back(random());
      }
    }

    void GetRandomKeyValue(std::string &key, std::string &value) {
      key=GetRandomKey();
      value=GetRandomVal();
    }

    void GetRandomKeyValues(std::vector<std::string> &v_key,std::vector<std::string> &v_value, int key_size, int value_size, int n) {
      for (int i = 0;i < n; i++) {
        v_key.push_back(GetRandomKey(key_size));
        v_value.push_back(GetRandomVal(value_size));
      }
    }

    void GetRandomKeyValues(std::vector<std::string> &v_key, std::vector<std::string> &v_value, int n) {
      for (int i = 0;i < n; i++) {
        v_key.push_back(GetRandomKey());
        v_value.push_back(GetRandomVal());
      }
    }

    void GetRandomKVs(std::vector<nemo::KV> &kv, int key_size, int value_size, int n) {
      for (int i = 0; i < n; i++) {
        nemo::KV temp;
        temp.key = GetRandomKey(key_size);
        temp.val = GetRandomVal(value_size);
        kv.push_back(temp);
      }

    }	

    void Usrand() {
      struct timeval now;
      gettimeofday(&now,NULL);
      srand(now.tv_usec);
    }

  private:

    static const int charsSetLen_ = 62;
    std::string charSet="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz";
    static const  int maxKeyLen_ = 254;
    static const  int minKeyLen_ = 0;
    static const  int maxFieldLen_ = 1024000;
    static const  int minFieldLen_ = 0;
    static const  int maxValLen_ = 1024000;
    static const  int minValLen_ = 1;
    static const  int charSetLen_ = 62;
};






#endif


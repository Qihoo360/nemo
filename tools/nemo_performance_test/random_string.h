#ifndef RANDOM_STRING_H_
#define RANDOM_STRING_H_
#include <cmath>
#include <string>
#include <sys/time.h>
#include <stdint.h>

#include "nemo_const.h"

class random_string {
public:
	random_string(){usrand();}
	~random_string(){}

	std::string getRandomChars(const  int length){
    //usrand();
		std::string randomStr;
		for( int index = 0; index < length; index++)
		{
			randomStr+=charSet[random()%charSetLen_];
		}
		return randomStr;
	}

	std::string getRandomChars(const  int min,const  int max){
		//usrand();
		int len=min + random()%(max - min + 1);
		return getRandomChars(len);
	}

  int32_t getRandomInt(){
   // usrand();
    return random();
  }
	
  std:: string getRandomBytes(const  int length){
		//usrand();
		std::string randomStr;
		for(  int index = 0;index<length;index++)
		{
			randomStr+=(char)(random()%256);
		}
		return randomStr;
	}

	std::string getRandomBytes(const  int min,const  int max){
		//usrand();
		int len = min + random()%( max - min + 1);
		return getRandomBytes(len);
	}
	
	std::string getRandomKey(){
		return getRandomBytes(minKeyLen_,maxKeyLen_);
	}
	
	std::string getRandomVal(){
		return getRandomBytes(minValLen_,maxValLen_);
	}
	
	std::string getRandomField(){
		return getRandomBytes(minFieldLen_,maxFieldLen_);
	}
	
	std::string getRandomKey( int length ){
		return getRandomBytes(length);
	}
	
	std::string getRandomVal( int length ){
		return getRandomBytes(length);
	}

	std::string getRandomField( int length ){
        	return this->getRandomBytes(length);
  }

	std::vector<std::string>& getRandomKeys(std::vector<std::string>&v_key, int playload, int n){
		for(int i=0;i<n;i++){
		    v_key.push_back(getRandomKey(playload));
		}
		    return v_key;
	}
	
	std::vector<std::string>& getRandomKeys(std::vector<std::string> &v_key,int n){
		for(int i=0;i<n;i++){
		    v_key.push_back(getRandomKey());
		}
		return v_key;
	}	
	
	
	std::vector<std::string>& getRandomVals(std::vector<std::string> &v_value,int playload,int n){
		for(int i=0;i<n;i++){
		    v_value.push_back(getRandomVal(playload));
		}
		return v_value;
	}
	
	std::vector<std::string>& getRandomVals(std::vector<std::string> &v_value,int n){
		for(int i=0;i<n;i++){
		    v_value.push_back(getRandomVal());
		}
		return v_value;
	}

		
	std::vector<std::string>& getRandomFields(std::vector<std::string> &v_field,int playload,int n){
		for(int i=0;i<n;i++){
		    v_field.push_back(getRandomField(playload));
		}
		    return v_field;
	}

	std::vector<std::string>& getRandomFields(std::vector<std::string> &v_field,int n){
		for(int i=0;i<n;i++){
		    v_field.push_back(getRandomField());
		}
		    return v_field;
	}

  void getRandomInts(std::vector<int64_t> &v_int,int n){
     usrand();
    for(int i=0;i<n;i++){
      v_int.push_back(random());
    }
  }

	void getRandomKeyValue(std::string &key, std::string &value){
		key=getRandomKey();
		value=getRandomVal();
	}

	void getRandomKeyValues(std::vector<std::string> &v_key,std::vector<std::string> &v_value,int playload,int valueSize,int n){
	    for(int i=0;i<n;i++){
		 v_key.push_back(getRandomKey(playload));
		 v_value.push_back(getRandomVal(valueSize));
	    }
	}

	void getRandomKeyValues(std::vector<std::string> &v_key,std::vector<std::string> &v_value,int n){
	    for(int i=0;i<n;i++){
		 v_key.push_back(getRandomKey());
		 v_value.push_back(getRandomVal());
	    }
	}

  void getRandomKVs(std::vector<nemo::KV> &kv,int playLoad,int valueSize,int n){
    for(int i = 0; i < n; i++){
      nemo::KV temp;
      temp.key = getRandomKey(playLoad);
      temp.val = getRandomVal(valueSize);
      kv.push_back(temp);
    }
  
  }	

	void usrand(){
		struct timeval now;
		gettimeofday(&now,NULL);
		srand(now.tv_usec);
	}


private:
	static const int charsSetLen_=62;
 	std::string charSet="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz";
	static const  int maxKeyLen_ = 254;
	static const  int minKeyLen_ =0;
	static const  int maxFieldLen_=1024000;
	static const  int minFieldLen_=0;
	static const  int maxValLen_=1024000;
	static const  int minValLen_=1;
	static const  int charSetLen_=62;
};






#endif


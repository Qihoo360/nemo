GCC = g++
CPPFLAGS = -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX  -DOS_LINUX -Wall -W -Wno-unused-parameter -DDEBUG -D__XDEBUG__ -g -O2 -std=c++11
OBJECT = compact

LIB_PATH = -L ../../output/lib
			
LIBS = -Wl,-Bstatic -lnemo -lnemodb -lrocksdb \
	   -Wl,-Bdynamic -lpthread\
	   -lsnappy \
	   -lrt \
	   -lz \
	   -lbz2

INCLUDE_PATH = -I../../output/include/ \
							 -I../../3rdparty/nemo-rocksdb/rocksdb/ \
							 -I../../3rdparty/nemo-rocksdb/rocksdb/include

.PHONY: all clean


# BASE_BOJS := $(wildcard *.cpp)
# BASE_BOJS += $(wildcard *.c)
# OBJS := $(patsubst %.cpp,%.o,$(BASE_BOJS)) 


all: $(OBJECT)
	rm *.o

$(OBJECT): $(OBJECT).o
	$(GCC) $(CPPFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

%.o : %.cc
	$(GCC) $(CPPFLAGS) -c $< -o $@ $(INCLUDE_PATH)

clean:
	rm -rf $(OBJECT) $(OBJECT).o

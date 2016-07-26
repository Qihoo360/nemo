GCC = g++
#CPPFLAGS = -Wall -W -Wno-unused-parameter -g -O2 -std=c++11
CPPFLAGS = -Wall -W -Wno-unused-parameter -DDEBUG -D__XDEBUG__ -g -O2 -std=c++11
OBJECT = p2r p2r_test_correct 

LIB_PATH = -L ../../output/lib \
					 -L ../../../pink/output/lib/ \
			
LIBS = -Wl,-Bstatic -lnemo -lrocksdb\
	   -Wl,-Bdynamic -lpthread\
	   -lsnappy \
	   -lrt \
	   -lz \
	   -lbz2 \
		 -lpink

INCLUDE_PATH = -I../../include/ \
							 -I../../3rdparty/rocksdb/include/ \
							 -I../../../pink/output/include/

#pika/third/pink/include


.PHONY: all clean


# BASE_BOJS := $(wildcard *.cpp)
# BASE_BOJS += $(wildcard *.c)
# OBJS := $(patsubst %.cpp,%.o,$(BASE_BOJS)) 


all: $(OBJECT)
	rm *.o

#$(OBJECT): $(OBJECT).o
#p2r : p2r.o worker_thread.o parse_thread.o sender_thread.o
p2r : p2r.o migrator_thread.o sender_thread.o parse_thread.o 
	$(GCC) $(CPPFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

p2r_test_correct : p2r_test_correct.o
	$(GCC) $(CPPFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

%.o : %.cc
	$(GCC) $(CPPFLAGS) -c $< -o $@ $(INCLUDE_PATH)

clean:
	rm -rf $(OBJECT) $(OBJECT).o

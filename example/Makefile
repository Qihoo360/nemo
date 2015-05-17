CXX = g++
CXXFLAGS = -Wall -DDEBUG -g -O2 -std=c++11 -D__XDEBUG__

OBJECT = main

LIB_PATH = -L../output/lib/ \
		   -L../3rdparty/rocksdb/ \

LIBS = -lpthread \
	   -lnemo \
	   -lrocksdb \
	   -lz \
	   -lbz2

INCLUDE_PATH = -I../include/ \
			   -I$(ROCKSDB_PATH)/include/ \
			   -I$(ROCKSDB_PATH)/include/rocksdb \
			   -I../src/

OBJS = main.o

.PHONY: all clean

all: $(OBJECT)
	@echo "Success, go, go, go..."


$(OBJECT): main.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

clean: 
	rm -rf $(OBJECT)
	rm -rf $(OBJECT)


CXX = g++
CXXFLAGS = -Wall -DDEBUG -g -O0 -std=c++11 -D__XDEBUG__

OBJECT = main test_bgsave zset

LIB_PATH = -L../output/lib/

LIBS = -lpthread \
	   -lnemo \
	   -lrocksdb \
	   -lz \
	   -lbz2 \
	   -lsnappy \
	   -lrt

INCLUDE_PATH = -I../output/include/

OBJS = main.o test_bgsave.o zset.o

.PHONY: all clean

all: $(OBJECT)
	@echo "Success, go, go, go..."

main: main.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

zset: zset.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

test_bgsave: test_bgsave.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

clean: 
	rm -rf $(OBJECT)
	rm -f $(OBJS)


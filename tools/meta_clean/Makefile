RPATH = /usr/local/pika20/lib/
LFLAGS = -Wl,-rpath=$(RPATH)

CXX = g++
CXXFLAGS = -Wall -Wno-format -DDEBUG -g -O0 -std=c++11 -D__XDEBUG__

OBJECT = meta_clean

LIB_PATH = -L../../output/lib/

LIBS = -lnemo \
	-lrocksdb \
	-lz \
	-lbz2 \
	-lsnappy \
	-lrt \
	-lpthread

INCLUDE_PATH = -I../../output/include/

OBJS = meta_clean.o

.PHONY: all clean

all: $(OBJECT)
	@echo "Success, go, go, go..."

meta_clean: meta_clean.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LFLAGS) $(LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

clean: 
	rm -rf $(OBJECT)
	rm -f $(OBJS)


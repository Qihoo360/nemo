CXX = g++
CXXFLAGS = -Wall -DDEBUG -g -O0 -std=c++11 -D__XDEBUG__

RPATH = /usr/local/pika11/lib/
LFLAGS = -Wl,-rpath=$(RPATH)

OBJECT = migrator

LIB_PATH = -L../output/lib/

LIBS = -lpthread \
	   -lnemo \
	   -lrocksdb \
	   -lz \
	   -lbz2 \
	   -lsnappy \
	   -lrt

INCLUDE_PATH = -I../output/include/

OBJS = migrator.o

.PHONY: all clean

all: $(OBJECT)
	@echo "Success, go, go, go..."

migrator: migrator.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LFLAGS) $(LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

clean: 
	rm -rf $(OBJECT)
	rm -f $(OBJS)


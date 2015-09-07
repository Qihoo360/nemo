CXX = g++

ifeq ($(__PERF), 1)
	CXXFLAGS = -O0 -g -pg -pipe -fPIC -D__XDEBUG__ -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -Wconversion -D_GNU_SOURCE
else
	# CXXFLAGS = -O2 -g -pipe -fPIC -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -Wconversion -D_GNU_SOURCE
	CXXFLAGS = -Wall -W -DDEBUG -g -O0 -D__XDEBUG__ -D__STDC_FORMAT_MACROS -fPIC -std=c++11
endif

OBJECT = nemo
SRC_DIR = ./src
OUTPUT = ./output

LIB_PATH = -L./
LIBS = -lpthread

ROCKSDB_PATH = ./3rdparty/rocksdb/


INCLUDE_PATH = -I./include/ \
			   -I$(ROCKSDB_PATH)/include/

LIBRARY = libnemo.a

.PHONY: all clean


BASE_OBJS := $(wildcard $(SRC_DIR)/*.cc)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.c)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst %.cc,%.o,$(BASE_OBJS))

all: $(LIBRARY)
	@echo "Success, go, go, go..."


$(LIBRARY): $(OBJS)
	make -C $(ROCKSDB_PATH) static_lib
	rm -rf $(OUTPUT)
	mkdir $(OUTPUT)
	mkdir $(OUTPUT)/include
	mkdir $(OUTPUT)/lib
	rm -rf $@
	ar -rcs $@ $(OBJS)
	cp -rf $(ROCKSDB_PATH)/include/* $(OUTPUT)/include
	cp -rf ./include/* $(OUTPUT)/include
	mv ./libnemo.a $(OUTPUT)/lib/
	cp $(ROCKSDB_PATH)/librocksdb.a $(OUTPUT)/lib
	make -C example

$(OBJECT): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

$(TOBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

clean: 
#	make -C $(ROCKSDB_PATH) clean
	make -C example clean
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(OUTPUT)
	rm -rf $(LIBRARY)
	rm -rf $(OBJECT)


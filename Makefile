CXX = g++

ifeq ($(__PERF), 1)
	CXXFLAGS = -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX  -DOS_LINUX -O0 -g -gstabs+ -pg -pipe -fPIC -D__XDEBUG__ -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -Wconversion -Wno-unused-parameter -D_GNU_SOURCE -std=c++11
else
	# CXXFLAGS = -O2 -g -pipe -fPIC -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -Wconversion -D_GNU_SOURCE
	CXXFLAGS = -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX  -DOS_LINUX -Wall -W -Wno-unused-parameter -g -O2 -D__STDC_FORMAT_MACROS -fPIC -std=c++11
endif

OBJECT = nemo
SRC_DIR = ./src
OUTPUT = ./output

ROCKSDB_PATH = ./3rdparty/nemo-rocksdb

LIB_PATH = -L./ \
					 -L$(ROCKSDB_PATH)/output/lib
LIBS = -lpthread -lnemodb -lrocksdb


# tools
TOOLS_COMPACT_PATH = ./tools/compact
TOOLS_COMPACT_OBJ = compact
TOOLS_METASCAN_PATH = ./tools/meta_scan
TOOLS_METASCAN_OBJ = meta_scan
TOOLS_NEMOCK_PATH = ./tools/nemock
TOOLS_NEMOCK_OBJ = nemock

INCLUDE_PATH = -I./include/ \
			   			 -I$(ROCKSDB_PATH)/output/include \
							 -I$(ROCKSDB_PATH)/rocksdb \
							 -I$(ROCKSDB_PATH)/rocksdb/include

LIBRARY = libnemo.a
ROCKSDB = libnemodb.a

.PHONY: all clean


BASE_OBJS := $(wildcard $(SRC_DIR)/*.cc)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.c)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst %.cc,%.o,$(BASE_OBJS))

all: $(ROCKSDB) $(LIBRARY)
	@echo "Success, go, go, go..."

$(ROCKSDB):
	make -C $(ROCKSDB_PATH)

$(LIBRARY): $(OBJS)
	rm -rf $(OUTPUT)
	mkdir $(OUTPUT)
	mkdir $(OUTPUT)/include
	mkdir $(OUTPUT)/lib
	mkdir $(OUTPUT)/tools
	rm -rf $@
	ar -rcs $@ $(OBJS)
	cp -rf $(ROCKSDB_PATH)/include/* $(OUTPUT)/include
	cp -rf ./include/* $(OUTPUT)/include
	mv ./libnemo.a $(OUTPUT)/lib/
	cp $(ROCKSDB_PATH)/output/lib/libnemodb.a $(OUTPUT)/lib
	cp $(ROCKSDB_PATH)/output/lib/librocksdb.a $(OUTPUT)/lib
	# tools
	$(MAKE) -C $(TOOLS_COMPACT_PATH) $(TOOLS_COMPACT_OBJ)
	$(MAKE) -C $(TOOLS_METASCAN_PATH) $(TOOLS_METASCAN_OBJ)
	$(MAKE) -C $(TOOLS_NEMOCK_PATH) $(TOOLS_NEMOCK_OBJ)
	mv $(TOOLS_COMPACT_PATH)/$(TOOLS_COMPACT_OBJ) $(OUTPUT)/tools
	mv $(TOOLS_METASCAN_PATH)/$(TOOLS_METASCAN_OBJ) $(OUTPUT)/tools
	mv $(TOOLS_NEMOCK_PATH)/$(TOOLS_NEMOCK_OBJ) $(OUTPUT)/tools
	make -C example

$(OBJECT): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

$(TOBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

clean: 
	make -C $(ROCKSDB_PATH) clean
	make -C example clean
	$(MAKE) -C $(TOOLS_COMPACT_PATH) clean
	$(MAKE) -C $(TOOLS_METASCAN_PATH) clean
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(OUTPUT)
	rm -rf $(LIBRARY)
	rm -rf $(OBJECT)

distclean: clean 
	make clean -C $ROCKSDB_PATH)/rocksdb


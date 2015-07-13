#ifndef NEMO_INCLUDE_NEMO_OPTION_H_
#define NEMO_INCLUDE_NEMO_OPTION_H_

namespace nemo{

struct Options{
	std::string path;

	// In MBs.
	int cache_size;
	// Default: false
	bool compression;
	
	Options(){
		cache_size = 8;
		compression = false;
	}
};

}; // end namespace nemo

#endif

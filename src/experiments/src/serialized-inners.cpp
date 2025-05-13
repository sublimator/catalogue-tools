/**
 * serialized-inners.cpp
 * 
 * An experimental tool for exploring serialization approaches for SHAMap inner nodes.
 * This tool is used to evaluate different strategies for efficiently representing
 * and serializing the inner node structure of SHAMaps.
 */

#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <fstream>
#include <chrono>
#include <iomanip>

#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>

#include "catl/core/log-macros.h"
#include "catl/core/types.h"
#include "catl/shamap/shamap.h"
#include "catl/shamap/shamap-innernode.h"
#include "catl/shamap/shamap-nodetype.h"
#include "catl/v1/catl-v1-reader.h"
#include "catl/v1/catl-v1-writer.h"

using namespace catl::v1;
namespace po = boost::program_options;
namespace fs = boost::filesystem;

/**
 * Main entry point
 */
int main(int argc, char* argv[]) 
{
    Logger::set_level(LogLevel::INFO);
    LOGI("Experiment set up");
}

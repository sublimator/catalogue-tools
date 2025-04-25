#include <fstream>
#include <gtest/gtest.h>
#include <iostream>
#include <string>

#include "utils/test-utils.h"

// Simple test to verify our path resolution works
TEST(FilePathTest, FindTestDataFile)
{
    // Get the path to the test data file relative to this source file
    std::string file_path = TestDataPath::get_path("fixture/op-adds.json");
    std::cout << "Test data path: " << file_path << std::endl;

    // Verify the file exists
    std::ifstream file(file_path);
    EXPECT_TRUE(file.good())
        << "Could not find test data file at: " << file_path
        << "\nMake sure to create a 'fixture' directory next to this source "
           "file.";
}

// This will print the current source directory for debugging
TEST(FilePathTest, PrintSourceDirectory)
{
    std::cout << "Current source directory: " << CURRENT_SOURCE_DIR
              << std::endl;
}

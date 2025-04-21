#include "catl/shamap/shamap-errors.h"
#include <cstddef>
#include <stdexcept>
#include <string>

//----------------------------------------------------------
// Exception Classes Implementation
//----------------------------------------------------------
SHAMapException::SHAMapException(const std::string& message)
    : std::runtime_error(message)
{
}

InvalidDepthException::InvalidDepthException(int depth, size_t maxAllowed)
    : SHAMapException(
          "Invalid depth (" + std::to_string(depth) +
          ") for key in selectBranch. Max allowed: " +
          std::to_string(maxAllowed))
    , depth_(depth)
    , maxAllowed_(maxAllowed)
{
}

int
InvalidDepthException::depth() const
{
    return depth_;
}

size_t
InvalidDepthException::max_allowed() const
{
    return maxAllowed_;
}

InvalidBranchException::InvalidBranchException(int branch)
    : SHAMapException("Invalid branch index: " + std::to_string(branch))
    , branch_(branch)
{
}

int
InvalidBranchException::branch() const
{
    return branch_;
}

NullNodeException::NullNodeException(const std::string& context)
    : SHAMapException("Null node encountered: " + context)
{
}

NullItemException::NullItemException()
    : SHAMapException("Found leaf node with null item")
{
}

HashCalculationException::HashCalculationException(const std::string& reason)
    : SHAMapException("Hash calculation error: " + reason)
{
}
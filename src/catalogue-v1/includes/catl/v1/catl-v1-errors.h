#pragma once

#include <stdexcept>
#include <string>

namespace catl::v1 {

// Base exception for CATL v1 errors
class CatlV1Error : public std::runtime_error
{
public:
    explicit CatlV1Error(const std::string& msg) : std::runtime_error(msg)
    {
    }
};

// Exception for unsupported version
class CatlV1UnsupportedVersionError : public CatlV1Error
{
public:
    explicit CatlV1UnsupportedVersionError(const std::string& msg)
        : CatlV1Error(msg)
    {
    }
};

// Exception for invalid CATL header
class CatlV1InvalidHeaderError : public CatlV1Error
{
public:
    explicit CatlV1InvalidHeaderError(const std::string& msg) : CatlV1Error(msg)
    {
    }
};

// Exception for file size mismatch
class CatlV1FileSizeMismatchError : public CatlV1Error
{
public:
    explicit CatlV1FileSizeMismatchError(const std::string& msg)
        : CatlV1Error(msg)
    {
    }
};

// Exception for hash verification failure
class CatlV1HashVerificationError : public CatlV1Error
{
public:
    explicit CatlV1HashVerificationError(const std::string& msg)
        : CatlV1Error(msg)
    {
    }
};

}  // namespace catl::v1

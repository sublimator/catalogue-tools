#pragma once

#include <stdexcept>

//----------------------------------------------------------
// Custom Exception Classes
//----------------------------------------------------------
class SHAMapException : public std::runtime_error
{
public:
    explicit SHAMapException(const std::string& message);
};

class InvalidDepthException : public SHAMapException
{
public:
    explicit InvalidDepthException(int depth, size_t maxAllowed);
    int
    depth() const;
    size_t
    max_allowed() const;

private:
    int depth_;
    size_t max_allowed_;
};

class InvalidBranchException : public SHAMapException
{
public:
    explicit InvalidBranchException(int branch);
    int
    branch() const;

private:
    int branch_;
};

class NullNodeException : public SHAMapException
{
public:
    explicit NullNodeException(const std::string& context);
};

class NullItemException : public SHAMapException
{
public:
    explicit NullItemException();
};

class HashCalculationException : public SHAMapException
{
public:
    explicit HashCalculationException(const std::string& reason);
};
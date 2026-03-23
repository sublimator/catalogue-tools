#pragma once

#include <stdexcept>
#include <string>

namespace catl::xdata {

enum class CodecErrorCode
{
    invalid_value,    // wrong JSON type or unparseable value
    missing_field,    // required sub-field absent (e.g. IOU without issuer)
    out_of_range,     // value exceeds type bounds (e.g. negative native amount)
    invalid_encoding, // bad hex string, bad base58, wrong length
    unknown_field,    // field name not found in Protocol
    unknown_enum,     // string enum value not found (e.g. bad TransactionType)
    malformed_data,   // binary structure corrupt (decode side)
};

class CodecError : public std::runtime_error
{
public:
    CodecErrorCode code;
    std::string type;
    std::string path;

protected:
    CodecError(
        std::string prefix,
        CodecErrorCode code_,
        std::string type_,
        std::string msg,
        std::string path_)
        : std::runtime_error(
              prefix + " " + type_ + ": " + msg +
              (path_.empty() ? "" : " at " + path_))
        , code(code_)
        , type(std::move(type_))
        , path(std::move(path_))
    {
    }
};

class EncodeError : public CodecError
{
public:
    EncodeError(
        CodecErrorCode code_,
        std::string type_,
        std::string msg,
        std::string path_ = {})
        : CodecError(
              "encode",
              code_,
              std::move(type_),
              std::move(msg),
              std::move(path_))
    {
    }
};

class DecodeError : public CodecError
{
public:
    DecodeError(
        CodecErrorCode code_,
        std::string type_,
        std::string msg,
        std::string path_ = {})
        : CodecError(
              "decode",
              code_,
              std::move(type_),
              std::move(msg),
              std::move(path_))
    {
    }
};

}  // namespace catl::xdata

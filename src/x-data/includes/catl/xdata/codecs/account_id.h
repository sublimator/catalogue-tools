#pragma once

#include "catl/base58/base58.h"
#include "catl/xdata/codec-error.h"
#include "catl/xdata/serializer.h"
#include <boost/json.hpp>

namespace catl::xdata::codecs {

struct AccountIDCodec
{
    static constexpr size_t fixed_size = 20;

    static constexpr std::string_view ZERO_ACCOUNT_B58 =
        "rrrrrrrrrrrrrrrrrrrrrhoLvTp";

    // An account field has two wire forms, and rippled's JSON tells them
    // apart (STAccount::getText):
    //   - a field that was never set is an empty field (length 0), and its
    //     JSON is the empty string. Pseudo-transactions whose Account nothing
    //     assigns have it: UNLModify, and Xahau's Entropy in every ledger;
    //   - a field set to an account is twenty bytes, and its JSON is the
    //     base58 address. That includes the zero account, when it was set:
    //     an EnableAmendment or SetFee, a payment to the zero account.
    // So "" is the empty field and every address, the zero one too, is twenty
    // bytes. rippled's own JSON reader refuses ""; this codec reads it,
    // because it is what the empty field decodes to and a leaf has to
    // serialize back to the bytes it came from.
    static size_t
    encoded_size(boost::json::value const& v)
    {
        if (v.is_string() && v.as_string().empty())
            return 0;
        return 20;
    }

    // From raw 20 bytes
    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, std::span<const uint8_t, 20> data)
    {
        s.add_raw(std::span<const uint8_t>{data.data(), 20});
    }

    // Check if 20 bytes are all zero (default/absent account)
    static bool
    is_zero_account(const uint8_t* data, size_t size)
    {
        if (size != 20)
            return false;
        for (size_t i = 0; i < 20; ++i)
        {
            if (data[i] != 0)
                return false;
        }
        return true;
    }

    // From its JSON string. The empty string writes nothing: the caller
    // (STObjectCodec) has written a length of 0. An address writes its
    // twenty bytes.
    template <ByteSink Sink>
    static void
    encode(
        Serializer<Sink>& s,
        std::string_view base58_addr,
        std::string const& path = {})
    {
        if (base58_addr.empty())
            return;

        auto decoded = base58::decode_account_id(base58_addr);
        if (!decoded || decoded->size() != 20)
        {
            throw EncodeError(
                CodecErrorCode::invalid_encoding,
                "AccountID",
                "invalid base58 address: " + std::string(base58_addr),
                path);
        }
        s.add_raw(std::span<const uint8_t>{decoded->data(), decoded->size()});
    }

    // From JSON (base58 string)
    template <ByteSink Sink>
    static void
    encode(
        Serializer<Sink>& s,
        boost::json::value const& v,
        std::string const& path = {})
    {
        if (!v.is_string())
        {
            throw EncodeError(
                CodecErrorCode::invalid_value,
                "AccountID",
                "expected string",
                path);
        }
        encode(s, std::string_view(v.as_string()), path);
    }

    static boost::json::value
    decode(Slice const& data)
    {
        // An empty field is an account that was never set: "", as rippled
        // prints it. Twenty zero bytes are the zero account's address.
        if (data.empty())
            return boost::json::string("");
        return boost::json::string(
            base58::encode_account_id(data.data(), data.size()));
    }
};

}  // namespace catl::xdata::codecs

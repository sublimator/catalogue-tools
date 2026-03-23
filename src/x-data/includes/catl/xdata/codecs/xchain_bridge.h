#pragma once

#include "catl/xdata/codecs/account_id.h"
#include "catl/xdata/codecs/issue.h"
#include "catl/xdata/serializer.h"
#include <boost/json.hpp>

namespace catl::xdata::codecs {

// XChainBridge wire format:
//   LockingChainDoor  (AccountID, 20 bytes)
//   LockingChainIssue (Issue, 20 or 40 bytes)
//   IssuingChainDoor  (AccountID, 20 bytes)
//   IssuingChainIssue (Issue, 20 or 40 bytes)
struct XChainBridgeCodec
{
    static size_t
    encoded_size(boost::json::value const& v)
    {
        auto const& obj = v.as_object();
        // Doors: VL prefix (1 byte) + 20 bytes AccountID
        // Issues: raw 20 or 40 bytes (no VL)
        size_t lcd_size = AccountIDCodec::encoded_size(obj.at("LockingChainDoor"));
        size_t icd_size = AccountIDCodec::encoded_size(obj.at("IssuingChainDoor"));
        return vl_prefix_size(lcd_size) + lcd_size +
               IssueCodec::encoded_size(obj.at("LockingChainIssue")) +
               vl_prefix_size(icd_size) + icd_size +
               IssueCodec::encoded_size(obj.at("IssuingChainIssue"));
    }

    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, boost::json::value const& v)
    {
        auto const& obj = v.as_object();
        // Doors are STAccount → VL-encoded (like in STObject)
        auto lcd_sv = std::string_view(obj.at("LockingChainDoor").as_string());
        size_t lcd_size = (lcd_sv == AccountIDCodec::ZERO_ACCOUNT_B58) ? 0 : 20;
        s.add_vl_prefix(lcd_size);
        AccountIDCodec::encode(s, lcd_sv);

        // Issues are STIssue → raw (no VL)
        IssueCodec::encode(s, obj.at("LockingChainIssue"));

        auto icd_sv = std::string_view(obj.at("IssuingChainDoor").as_string());
        size_t icd_size = (icd_sv == AccountIDCodec::ZERO_ACCOUNT_B58) ? 0 : 20;
        s.add_vl_prefix(icd_size);
        AccountIDCodec::encode(s, icd_sv);

        IssueCodec::encode(s, obj.at("IssuingChainIssue"));
    }

    static boost::json::value
    decode(Slice const& data)
    {
        boost::json::object obj;
        // LockingChainDoor: 20 bytes
        obj["LockingChainDoor"] = AccountIDCodec::decode(
            Slice(data.data(), 20));
        size_t pos = 20;
        // LockingChainIssue: 20 or 40 bytes
        Slice lci_slice(data.data() + pos, data.size() - pos);
        auto lci_parsed = parse_issue(lci_slice);
        size_t lci_size = lci_parsed.is_native() ? 20 : 40;
        obj["LockingChainIssue"] = IssueCodec::decode(
            Slice(data.data() + pos, lci_size));
        pos += lci_size;
        // IssuingChainDoor: 20 bytes
        obj["IssuingChainDoor"] = AccountIDCodec::decode(
            Slice(data.data() + pos, 20));
        pos += 20;
        // IssuingChainIssue: rest
        obj["IssuingChainIssue"] = IssueCodec::decode(
            Slice(data.data() + pos, data.size() - pos));
        return obj;
    }
};

}  // namespace catl::xdata::codecs

#pragma once

#include "catl/xdata/codecs/account_id.h"
#include "catl/xdata/codecs/issue.h"
#include "catl/xdata/serializer.h"
#include "catl/xdata/types/issue.h"
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
        size_t pos = 0;

        // LockingChainDoor: STAccount = VL prefix + AccountID (20 bytes)
        size_t lcd_vl = data.data()[pos];
        pos += 1;
        obj["LockingChainDoor"] = AccountIDCodec::decode(
            Slice(data.data() + pos, lcd_vl));
        pos += lcd_vl;

        // LockingChainIssue: STIssue = 20 (XRP) or 40 (non-XRP) bytes
        SliceCursor lci_cursor(Slice(data.data() + pos, data.size() - pos));
        size_t lci_size = get_issue_size(lci_cursor);
        obj["LockingChainIssue"] = IssueCodec::decode(
            Slice(data.data() + pos, lci_size));
        pos += lci_size;

        // IssuingChainDoor: STAccount = VL prefix + AccountID
        size_t icd_vl = data.data()[pos];
        pos += 1;
        obj["IssuingChainDoor"] = AccountIDCodec::decode(
            Slice(data.data() + pos, icd_vl));
        pos += icd_vl;

        // IssuingChainIssue: STIssue = rest
        obj["IssuingChainIssue"] = IssueCodec::decode(
            Slice(data.data() + pos, data.size() - pos));
        return obj;
    }

    /// Get the total wire size by peeking at the data.
    static size_t
    wire_size(SliceCursor& cursor)
    {
        size_t start = cursor.pos;

        // LockingChainDoor: VL + account
        size_t lcd_len = cursor.data.data()[cursor.pos];
        cursor.pos += 1 + lcd_len;

        // LockingChainIssue: 20 or 40
        cursor.pos += get_issue_size(cursor);

        // IssuingChainDoor: VL + account
        size_t icd_len = cursor.data.data()[cursor.pos];
        cursor.pos += 1 + icd_len;

        // IssuingChainIssue: 20 or 40
        cursor.pos += get_issue_size(cursor);

        size_t total = cursor.pos - start;
        cursor.pos = start;  // reset — caller will read
        return total;
    }
};

}  // namespace catl::xdata::codecs

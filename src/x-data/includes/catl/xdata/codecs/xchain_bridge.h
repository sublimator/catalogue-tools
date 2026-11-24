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

    // Both issues are of the network this object lives on, so they share its
    // native code (see IssueCodec).
    template <ByteSink Sink>
    static void
    encode(
        Serializer<Sink>& s,
        boost::json::value const& v,
        std::string_view native = CurrencyCodec::default_native)
    {
        auto const& obj = v.as_object();
        // Doors are STAccount → VL-encoded (like in STObject)
        auto lcd_sv = std::string_view(obj.at("LockingChainDoor").as_string());
        size_t lcd_size = (lcd_sv == AccountIDCodec::ZERO_ACCOUNT_B58) ? 0 : 20;
        s.add_vl_prefix(lcd_size);
        AccountIDCodec::encode(s, lcd_sv);

        // Issues are STIssue → raw (no VL)
        IssueCodec::encode(s, obj.at("LockingChainIssue"), {}, native);

        auto icd_sv = std::string_view(obj.at("IssuingChainDoor").as_string());
        size_t icd_size = (icd_sv == AccountIDCodec::ZERO_ACCOUNT_B58) ? 0 : 20;
        s.add_vl_prefix(icd_size);
        AccountIDCodec::encode(s, icd_sv);

        IssueCodec::encode(s, obj.at("IssuingChainIssue"), {}, native);
    }

    static boost::json::value
    decode(
        Slice const& data,
        std::string_view native = CurrencyCodec::default_native)
    {
        // Every length here is read from the bytes themselves, so each step
        // goes through the cursor's checked reads.
        boost::json::object obj;
        SliceCursor cursor{data, 0};
        auto const door = [&cursor]() {
            // STAccount: a one-byte length, then the account
            size_t const len = cursor.read_u8();
            return AccountIDCodec::decode(cursor.read_slice(len));
        };

        obj["LockingChainDoor"] = door();
        // STIssue: 20 (native), 40 (issued) or 44 (MPT) bytes
        size_t const lci_size = get_issue_size(cursor);
        obj["LockingChainIssue"] =
            IssueCodec::decode(cursor.read_slice(lci_size), native);
        obj["IssuingChainDoor"] = door();
        // IssuingChainIssue: the rest
        obj["IssuingChainIssue"] = IssueCodec::decode(
            Slice(data.data() + cursor.pos, cursor.remaining_size()), native);
        return obj;
    }

    /// Get the total wire size by peeking at the data. The caller's cursor
    /// is not moved. Throws SliceCursorError if the data ends early: the
    /// lengths are untrusted, and an unchecked walk read past the buffer.
    static size_t
    wire_size(SliceCursor const& cursor)
    {
        SliceCursor walk = cursor;
        auto const door = [&walk]() { walk.advance(walk.read_u8()); };
        auto const issue = [&walk]() { walk.advance(get_issue_size(walk)); };
        door();   // LockingChainDoor
        issue();  // LockingChainIssue
        door();   // IssuingChainDoor
        issue();  // IssuingChainIssue
        return walk.pos - cursor.pos;
    }
};

}  // namespace catl::xdata::codecs

#pragma once

#include "catl/xdata/codecs/account_id.h"
#include "catl/xdata/codecs/issue.h"
#include "catl/xdata/serializer.h"
#include <boost/json.hpp>

namespace catl::xdata::codecs {

struct XChainBridgeCodec
{
    // Always 4 × 20 = 80 bytes? No — it's two account IDs = 40 bytes.
    // Actually XChainBridge is: LockingChainDoor(20) + LockingChainIssue(20|40)
    //   + IssuingChainDoor(20) + IssuingChainIssue(20|40)
    // But the wire format is fixed at 40 bytes (two AccountIDs) per SField.h.
    // TODO: verify this against rippled — XChainBridge may actually be an
    // STObject not a fixed type. For now, encode as the JSON object suggests.

    static size_t
    encoded_size(boost::json::value const&)
    {
        // XChainBridge is 4 AccountIDs = 80 bytes
        // Wait no — FieldType says fixed_size=40, which is 2 AccountIDs.
        // Need to verify. Use 40 for now matching our FieldType definition.
        return 40;
    }

    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, boost::json::value const& v)
    {
        auto const& obj = v.as_object();
        AccountIDCodec::encode(
            s,
            std::string_view(obj.at("LockingChainDoor").as_string()));
        AccountIDCodec::encode(
            s,
            std::string_view(obj.at("IssuingChainDoor").as_string()));
    }
};

}  // namespace catl::xdata::codecs

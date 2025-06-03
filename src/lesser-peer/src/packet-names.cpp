#include <algorithm>
#include <catl/peer/packet-names.h>
#include <cctype>

namespace catl::peer {

namespace {

struct packet_info
{
    packet_type type;
    std::string_view name;
    std::string_view padded_name;
};

constexpr packet_info packet_names[] = {
    {packet_type::manifests, "mtMANIFESTS", "mtMANIFESTS               "},
    {packet_type::ping, "mtPING", "mtPING                    "},
    {packet_type::cluster, "mtCLUSTER", "mtCLUSTER                 "},
    {packet_type::endpoints, "mtENDPOINTS", "mtENDPOINTS               "},
    {packet_type::transaction, "mtTRANSACTION", "mtTRANSACTION             "},
    {packet_type::get_ledger, "mtGET_LEDGER", "mtGET_LEDGER              "},
    {packet_type::ledger_data, "mtLEDGER_DATA", "mtLEDGER_DATA             "},
    {packet_type::propose_ledger,
     "mtPROPOSE_LEDGER",
     "mtPROPOSE_LEDGER          "},
    {packet_type::status_change,
     "mtSTATUS_CHANGE",
     "mtSTATUS_CHANGE           "},
    {packet_type::have_set, "mtHAVE_SET", "mtHAVE_SET                "},
    {packet_type::validation, "mtVALIDATION", "mtVALIDATION              "},
    {packet_type::get_objects, "mtGET_OBJECTS", "mtGET_OBJECTS             "},
    {packet_type::get_shard_info,
     "mtGET_SHARD_INFO",
     "mtGET_SHARD_INFO          "},
    {packet_type::shard_info, "mtSHARD_INFO", "mtSHARD_INFO              "},
    {packet_type::get_peer_shard_info,
     "mtGET_PEER_SHARD_INFO",
     "mtGET_PEER_SHARD_INFO     "},
    {packet_type::peer_shard_info,
     "mtPEER_SHARD_INFO",
     "mtPEER_SHARD_INFO         "},
    {packet_type::validator_list,
     "mtVALIDATORLIST",
     "mtVALIDATORLIST           "},
    {packet_type::squelch, "mtSQUELCH", "mtSQUELCH                 "},
    {packet_type::validator_list_collection,
     "mtVALIDATORLISTCOLLECTION",
     "mtVALIDATORLISTCOLLECTION "},
    {packet_type::proof_path_req,
     "mtPROOF_PATH_REQ",
     "mtPROOF_PATH_REQ          "},
    {packet_type::proof_path_response,
     "mtPROOF_PATH_RESPONSE",
     "mtPROOF_PATH_RESPONSE     "},
    {packet_type::replay_delta_req,
     "mtREPLAY_DELTA_REQ",
     "mtREPLAY_DELTA_REQ        "},
    {packet_type::replay_delta_response,
     "mtREPLAY_DELTA_RESPONSE",
     "mtREPLAY_DELTA_RESPONSE   "},
    {packet_type::get_peer_shard_info_v2,
     "mtGET_PEER_SHARD_INFO_V2",
     "mtGET_PEER_SHARD_INFO_V2  "},
    {packet_type::peer_shard_info_v2,
     "mtPEER_SHARD_INFO_V2",
     "mtPEER_SHARD_INFO_V2      "},
    {packet_type::have_transactions,
     "mtHAVE_TRANSACTIONS",
     "mtHAVE_TRANSACTIONS       "},
    {packet_type::transactions, "mtTRANSACTIONS", "mtTRANSACTIONS            "},
    {packet_type::resource_report,
     "mtRESOURCE_REPORT",
     "mtRESOURCE_REPORT         "}};

constexpr std::string_view unknown_packet = "mtUNKNOWN_PACKET";
constexpr std::string_view unknown_packet_padded = "mtUNKNOWN_PACKET          ";

bool
case_insensitive_compare(std::string_view a, std::string_view b)
{
    if (a.size() != b.size())
        return false;
    return std::equal(a.begin(), a.end(), b.begin(), [](char ca, char cb) {
        return std::tolower(ca) == std::tolower(cb);
    });
}

}  // anonymous namespace

std::string_view
packet_type_to_string(packet_type type, bool padded)
{
    for (auto const& info : packet_names)
    {
        if (info.type == type)
        {
            return padded ? info.padded_name : info.name;
        }
    }
    return padded ? unknown_packet_padded : unknown_packet;
}

std::optional<packet_type>
string_to_packet_type(std::string_view name)
{
    for (auto const& info : packet_names)
    {
        if (case_insensitive_compare(name, info.name))
        {
            return info.type;
        }
    }
    return std::nullopt;
}

}  // namespace catl::peer
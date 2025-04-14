#include "hasher/http/http-handler.h"
#include "../utils.h"

#include <regex>
#include <sstream>

void
LedgerRequestHandler::handle_health(AbstractResponse& res) const
{
    res.setStatus(200);  // OK
    res.setBody(
        "{\"status\": \"healthy\", \"ledgers\": " +
        std::to_string(ledgerStore_->size()) + "}");
}

void
LedgerRequestHandler::handle_ledger(
    const std::string& path,
    AbstractResponse& res) const
{
    // Extract ledger index from path
    std::regex ledger_regex("/ledger/(\\d+)");
    std::smatch match;

    if (std::regex_match(path, match, ledger_regex) && match.size() > 1)
    {
        uint32_t ledger_index = std::stoul(match[1].str());
        auto ledger = ledgerStore_->get_ledger(ledger_index);

        if (ledger)
        {
            // Ledger found, return its details
            const auto& header = ledger->header();
            std::ostringstream json;

            json << "{\n";
            json << "  \"ledger_index\": " << header.sequence() << ",\n";
            json << "  \"ledger_hash\": \"" << header.hash().hex() << "\",\n";
            json << "  \"parent_hash\": \"" << header.parent_hash().hex()
                 << "\",\n";
            json << "  \"account_hash\": \"" << header.account_hash().hex()
                 << "\",\n";
            json << "  \"transaction_hash\": \""
                 << header.transaction_hash().hex() << "\",\n";
            json << "  \"close_time_unix\": "
                 << utils::to_unix_time(header.close_time()) << ",\n";
            json << "  \"close_time_human\": \""
                 << utils::format_ripple_time(header.close_time()) << "\",\n";
            json << "  \"total_coins\": " << header.drops() << ",\n";
            json << "  \"close_flags\": "
                 << static_cast<int>(header.close_flags()) << "\n";
            json << "}";

            res.setStatus(200);  // OK
            res.setBody(json.str());
        }
        else
        {
            // Ledger not found
            res.setStatus(404);  // Not Found
            res.setBody(
                "{\"error\": \"Ledger not found\", \"requested_index\": " +
                std::to_string(ledger_index) + "}");
        }
    }
    else
    {
        // Invalid path format
        res.setStatus(400);  // Bad Request
        res.setBody(
            "{\"error\": \"Invalid ledger path. Use /ledger/{index}\"}");
    }
}

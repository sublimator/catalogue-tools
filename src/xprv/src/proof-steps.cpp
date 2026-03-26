#include "xprv/proof-steps.h"

namespace xprv {

static std::string
check_mark(Check c)
{
    switch (c)
    {
        case Check::pass:
            return "PASS";
        case Check::fail:
            return "FAIL";
        case Check::skip:
            return "—";
    }
    return "?";
}

static std::string
role_name(HeaderRole r)
{
    switch (r)
    {
        case HeaderRole::anchor:
            return "anchor";
        case HeaderRole::flag:
            return "flag";
        case HeaderRole::target:
            return "target";
    }
    return "unknown";
}

static std::string
skip_list_name(SkipListType t)
{
    switch (t)
    {
        case SkipListType::recent:
            return "recent ledger hashes (last 256 ledgers)";
        case SkipListType::flag:
            return "flag skip list (every 256th ledger in this range)";
    }
    return "skip list";
}

std::vector<std::string>
render_narrative(std::vector<ProofStep> const& steps)
{
    std::vector<std::string> lines;
    lines.push_back("How this proof works:");
    lines.push_back("");

    for (auto const& step : steps)
    {
        std::string prefix =
            "  Step " + std::to_string(step.step_number) + ": ";

        if (auto* a = std::get_if<AnchorStep>(&step.data))
        {
            lines.push_back(
                prefix + "ANCHOR (ledger " + std::to_string(a->ledger_index) +
                ")");

            // Sub-steps from AnchorVerifier
            for (auto const& sub : a->sub_steps)
            {
                lines.push_back("    " + sub);
            }

            if (a->quorum_check == Check::pass)
            {
                lines.push_back(
                    "    => Anchor hash " + a->ledger_hash +
                    "... is TRUSTED (" +
                    std::to_string(a->validations_matched) + "/" +
                    std::to_string(a->unl_size) + " validators)");
            }
            else if (a->quorum_check == Check::skip)
            {
                lines.push_back(
                    "    => Anchor hash " + a->ledger_hash +
                    "... (no trusted key provided, not verified)");
            }
        }
        else if (auto* h = std::get_if<HeaderStep>(&step.data))
        {
            lines.push_back(
                prefix + role_name(h->role) + " header" + " (ledger " +
                std::to_string(h->seq) + ")");

            // How was it verified?
            if (h->method == HeaderVerifyMethod::anchor_hash)
            {
                lines.push_back(
                    "    Hash " + h->computed_hash +
                    "... = SHA512Half(LWR\\0 || fields)");
                lines.push_back(
                    "    [" + check_mark(h->hash_check) +
                    "] matches anchor hash");
            }
            else if (h->method == HeaderVerifyMethod::skip_list)
            {
                lines.push_back(
                    "    Hash " + h->computed_hash +
                    "... = SHA512Half(LWR\\0 || fields)");
                lines.push_back(
                    "    [" + check_mark(h->hash_check) + "] found in " +
                    skip_list_name(h->skip_type) + " (" +
                    std::to_string(h->skip_list_size) + " entries)");
            }

            lines.push_back(
                "    => tx_hash=" + h->tx_hash +
                "...  account_hash=" + h->account_hash + "...");
        }
        else if (auto* m = std::get_if<MapProofStep>(&step.data))
        {
            std::string tree = m->is_state ? "state" : "tx";
            lines.push_back(prefix + tree + " tree proof");
            lines.push_back(
                "    Trie: " + std::to_string(m->inners) + " inners, " +
                std::to_string(m->placeholders) + " placeholders, " +
                std::to_string(m->leaves) + " leaf (depth " +
                std::to_string(m->max_depth) + ")");
            lines.push_back(
                "    [" + check_mark(m->root_hash_check) + "] root hash " +
                m->computed_root + "... matches " +
                (m->is_state ? "account_hash" : "tx_hash"));

            if (m->is_state && m->hashes_count > 0)
            {
                lines.push_back(
                    "    Leaf: LedgerHashes SLE (" +
                    skip_list_name(m->skip_type) + ") with " +
                    std::to_string(m->hashes_count) + " entries");
                lines.push_back(
                    "    => next header's hash must appear in this list");
            }
            else if (!m->tx_type.empty())
            {
                std::string leaf_desc = "    Leaf: " + m->tx_type;
                if (!m->tx_account.empty())
                {
                    leaf_desc += " from " + m->tx_account;
                }
                lines.push_back(leaf_desc);
            }
        }

        lines.push_back("");
    }

    return lines;
}

}  // namespace xprv

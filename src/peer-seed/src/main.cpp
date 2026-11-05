// peer-seed: seed an xrpld node's peer cache with the nearest peers that will
// take a connection.
//
// When a node starts, its gossip cache is empty, so its first outbound dials
// come from the bootcache in <data dir>/db/peerfinder.sqlite, highest valence
// first. `peer-seed seed` surveys the network over the peer protocol (see
// survey.h), ranks the peers that completed a handshake by median TMPing round
// trip, and writes the nearest into that table with valences above the
// current maximum, so the node dials them first. No node change is needed.
//
// Commands:
//   rank     survey and print the nearest peers. Writes nothing to the node.
//   seed     survey (or --from a saved survey), then write the chosen peers
//            into --db. The node must be stopped: it loads the table at start
//            and rewrites it while running.
//   check    read a running node's `peers` RPC and show where the seeds landed.
//   restore  copy the backup taken by the last `seed` back over --db.
//   probe    handshake and ping just the endpoints given; JSON lines out.
//
// Every handshake uses one persistent node identity,
// ~/.cache/catl-peer-seed/node-key, so peers see one node across runs.
// Backups and the last seed list live there too.

#include "bootcache.h"
#include "geo.h"
#include "http.h"
#include "survey.h"

#include <catl/core/logger.h>
#include <catl/peer-client/node-identity.h>
#include <catl/peer-client/peer-set.h>

#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/json.hpp>

#include <algorithm>
#include <charconv>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;
namespace asio = boost::asio;
using namespace peer_seed;

namespace {

constexpr uint32_t kStaleLedgers = 256;

struct Args
{
    std::string command;
    SurveyOptions survey;
    std::optional<fs::path> db;
    std::optional<fs::path> json_out;
    std::optional<fs::path> from;
    std::optional<fs::path> identity;
    std::vector<std::string> hubs;
    std::vector<std::string> endpoints;
    std::string rpc = "http://127.0.0.1:5005/";
    int count = 20;
    int per_prefix = 3;
    int per_as = 3;
    int show = 40;
    int measure_concurrency = 4;
    bool dry_run = false;
    bool events = false;
    bool geo = false;
};

fs::path
state_dir()
{
    char const* home = std::getenv("HOME");
    return fs::path(home ? home : ".") / ".cache" / "catl-peer-seed";
}

std::string
now_text(char const* format)
{
    auto const t = std::time(nullptr);
    std::tm local{};
    localtime_r(&t, &local);
    char buffer[64];
    std::strftime(buffer, sizeof buffer, format, &local);
    return buffer;
}

int
usage(std::ostream& out = std::cerr)
{
    out << R"(usage: peer-seed COMMAND [options]

  rank     [--db PATH] [--json OUT] [--events] [--show N]
  seed     --db PATH [--from SURVEY.json] [--dry-run] [--json OUT]
  check    [--rpc URL]            (default http://127.0.0.1:5005/)
  restore  --db PATH
  probe    host:port ...          (or one per line on stdin)

survey options (rank, seed, probe):
  --network-id N      network to survey (default 0, XRPL mainnet)
  --concurrency N     handshakes in flight (default 32)
  --pings N           TMPing round trips per ready peer (default 5)
  --timeout S         handshake and ping timeout, seconds (default 5)
  --gossip S          hold ready peers S seconds for TMEndpoints gossip
                      (default 0; nodes broadcast it every 151 s, so 160)
  --max-peers N       stop queueing new endpoints after N (default 20000)
  --no-crawl          don't ask peers for /crawl
  --hub HOST:PORT     another start point (repeatable)
  --ipv6              include IPv6 peers
  --identity PATH     node identity key file (default ~/.cache/catl-peer-seed/node-key)

  --measure-concurrency N
                      after the survey, re-time ready peers N at a time on a
                      quiet link (default 4; 0 keeps the survey's timings)

choosing seeds (rank, seed):
  --count N           peers to seed (default 20; the node has 21 slots)
  --per-prefix N      most seeds from one /16 (default 3)
  --per-as N          most seeds from one AS, with --geo (default 3)
  --geo               look peers up at ip-api.com (country, city, AS);
                      sends their public IPs there, cached in ~/.cache
)";
    return 2;
}

template <typename T>
bool
parse_number(std::string const& text, T& out)
{
    auto const [end, ec] =
        std::from_chars(text.data(), text.data() + text.size(), out);
    return ec == std::errc{} && end == text.data() + text.size();
}

std::optional<Args>
parse_args(int argc, char** argv)
{
    if (argc < 2)
        return std::nullopt;
    Args args;
    args.command = argv[1];
    for (int i = 2; i < argc; ++i)
    {
        std::string const arg = argv[i];
        auto next = [&]() -> std::optional<std::string> {
            if (i + 1 >= argc)
                return std::nullopt;
            return std::string(argv[++i]);
        };
        auto number = [&](auto& target) {
            auto text = next();
            return text && parse_number(*text, target);
        };
        bool ok = true;
        if (arg == "--network-id")
            ok = number(args.survey.network_id);
        else if (arg == "--concurrency")
            ok = number(args.survey.concurrency) && args.survey.concurrency > 0;
        else if (arg == "--pings")
            ok = number(args.survey.pings) && args.survey.pings >= 0;
        else if (arg == "--timeout")
            ok = number(args.survey.timeout_secs) &&
                args.survey.timeout_secs > 0;
        else if (arg == "--gossip")
            ok =
                number(args.survey.gossip_secs) && args.survey.gossip_secs >= 0;
        else if (arg == "--max-peers")
            ok = number(args.survey.max_peers);
        else if (arg == "--no-crawl")
            args.survey.crawl = false;
        else if (arg == "--ipv6")
            args.survey.ipv6 = true;
        else if (arg == "--count")
            ok = number(args.count) && args.count > 0;
        else if (arg == "--per-prefix")
            ok = number(args.per_prefix) && args.per_prefix > 0;
        else if (arg == "--measure-concurrency")
            ok = number(args.measure_concurrency) &&
                args.measure_concurrency >= 0;
        else if (arg == "--show")
            ok = number(args.show);
        else if (arg == "--dry-run")
            args.dry_run = true;
        else if (arg == "--events")
            args.events = true;
        else if (arg == "--geo")
            args.geo = true;
        else if (arg == "--per-as")
            ok = number(args.per_as) && args.per_as > 0;
        else if (
            arg == "--hub" || arg == "--db" || arg == "--json" ||
            arg == "--from" || arg == "--identity" || arg == "--rpc")
        {
            auto value = next();
            ok = value.has_value();
            if (ok && arg == "--hub")
                args.hubs.push_back(*value);
            else if (ok && arg == "--db")
                args.db = *value;
            else if (ok && arg == "--json")
                args.json_out = *value;
            else if (ok && arg == "--from")
                args.from = *value;
            else if (ok && arg == "--identity")
                args.identity = *value;
            else if (ok)
                args.rpc = *value;
        }
        else if (args.command == "probe" && arg.rfind("--", 0) != 0)
            args.endpoints.push_back(arg);
        else
            ok = false;
        if (!ok)
        {
            std::cerr << "bad argument: " << arg << "\n";
            return std::nullopt;
        }
    }
    return args;
}

/// /16 for IPv4 and /32 for IPv6: a rough stand-in for one operator.
std::string
prefix_of(std::string const& address)
{
    if (address.front() == '[')
    {
        auto const close = address.find(']');
        auto const ip = asio::ip::make_address_v6(address.substr(1, close - 1));
        auto bytes = ip.to_bytes();
        std::fill(bytes.begin() + 4, bytes.end(), 0);
        return asio::ip::address_v6(bytes).to_string() + "/32";
    }
    auto const ip =
        asio::ip::make_address_v4(address.substr(0, address.rfind(':')));
    auto bytes = ip.to_bytes();
    bytes[2] = bytes[3] = 0;
    return asio::ip::address_v4(bytes).to_string() + "/16";
}

std::string
ip_of(std::string const& address)
{
    if (address.front() == '[')
        return address.substr(1, address.find(']') - 1);
    return address.substr(0, address.rfind(':'));
}

/// Ready peers with pings, nearest first, minus any trailing the rest.
std::vector<PeerRecord>
rank_ready(std::vector<PeerRecord> const& records)
{
    uint32_t tip = 0;
    for (auto const& r : records)
    {
        if (r.outcome == "ready" && r.last_ledger)
            tip = std::max(tip, *r.last_ledger);
    }
    std::vector<PeerRecord> ready;
    for (auto const& r : records)
    {
        if (r.outcome != "ready" || r.ping_ms.empty())
            continue;
        if (r.last_ledger && tip > *r.last_ledger + kStaleLedgers)
            continue;
        ready.push_back(r);
    }
    std::stable_sort(
        ready.begin(), ready.end(), [](auto const& a, auto const& b) {
            return *a.ping_min() < *b.ping_min();
        });
    return ready;
}

/// Nearest first, with at most per_prefix from one /16, per_as from one AS
/// (when geo data says which), and one per IP.
std::vector<PeerRecord>
choose(std::vector<PeerRecord> const& ranked, Args const& args)
{
    std::vector<PeerRecord> chosen;
    std::map<std::string, int> prefixes;
    std::map<std::string, int> networks;
    std::set<std::string> ips;
    for (auto const& peer : ranked)
    {
        auto const prefix = prefix_of(peer.address);
        auto const ip = ip_of(peer.address);
        auto const as = as_number(peer.geo);
        if (ips.contains(ip) || prefixes[prefix] >= args.per_prefix ||
            (!as.empty() && networks[as] >= args.per_as))
            continue;
        chosen.push_back(peer);
        ++prefixes[prefix];
        if (!as.empty())
            ++networks[as];
        ips.insert(ip);
        if (static_cast<int>(chosen.size()) == args.count)
            break;
    }
    return chosen;
}

std::string
geo_text(boost::json::object const& geo, char const* key)
{
    auto const* value = geo.if_contains(key);
    return value && value->is_string() ? std::string(value->as_string()) : "";
}

/// Look up where each surveyed peer is, if --geo asked for it.
void
annotate_geo(Args const& args, std::vector<PeerRecord>& records)
{
    if (!args.geo)
        return;
    std::vector<std::string> ips;
    for (auto const& r : records)
        ips.push_back(ip_of(r.address));
    auto const found = geolocate(ips, state_dir() / "geo.json");
    for (auto& r : records)
    {
        if (auto const it = found.find(ip_of(r.address)); it != found.end())
            r.geo = it->second;
    }
}

std::size_t
distinct_prefixes(std::vector<PeerRecord> const& peers)
{
    std::set<std::string> prefixes;
    for (auto const& p : peers)
        prefixes.insert(prefix_of(p.address));
    return prefixes.size();
}

void
print_table(
    std::ostream& out,
    std::vector<PeerRecord> const& peers,
    std::set<std::string> const& chosen)
{
    out << std::setw(4) << "#" << std::setw(8) << "ping" << std::setw(8)
        << "median" << std::setw(8) << "shake"
        << "  " << std::left << std::setw(24) << "address" << std::setw(16)
        << "/16" << std::setw(14) << "server" << std::setw(11) << "ledger"
        << "where\n"
        << std::right;
    int i = 0;
    for (auto const& p : peers)
    {
        std::string where;
        if (!p.geo.empty())
        {
            where = geo_text(p.geo, "countryCode") + " " +
                geo_text(p.geo, "city") + ", " + geo_text(p.geo, "as");
            if (where.size() > 60)
                where = where.substr(0, 60);
        }
        out << std::setw(3) << ++i << (chosen.contains(p.address) ? "*" : " ")
            << std::setw(6) << std::fixed << std::setprecision(0)
            << *p.ping_min() << "ms" << std::setw(6) << *p.ping_median() << "ms"
            << std::setw(6) << p.handshake_ms.value_or(0) << "ms  " << std::left
            << std::setw(24) << p.address << std::setw(16)
            << prefix_of(p.address) << std::setw(14)
            << (p.server.empty() ? "?" : p.server) << std::setw(11)
            << (p.last_ledger ? std::to_string(*p.last_ledger) : "?") << where
            << "\n"
            << std::right;
    }
}

std::vector<std::pair<std::string, std::string>>
survey_seeds(Args const& args, std::vector<BootcacheEntry> const& bootcache)
{
    std::vector<std::pair<std::string, std::string>> seeds;
    std::vector<std::string> hubs;
    for (auto const& hub :
         catl::peer_client::get_bootstrap_peers(args.survey.network_id))
        hubs.push_back(hub.host + ":" + std::to_string(hub.port));
    hubs.insert(hubs.end(), args.hubs.begin(), args.hubs.end());

    // Hubs are hostnames; the bootcache holds addresses, so resolve them.
    asio::io_context io;
    asio::ip::tcp::resolver resolver(io);
    for (auto const& hub : hubs)
    {
        auto const colon = hub.rfind(':');
        auto const host = hub.substr(0, colon);
        auto const port = colon == std::string::npos ? std::string("51235")
                                                     : hub.substr(colon + 1);
        boost::system::error_code ec;
        auto const results = resolver.resolve(host, port, ec);
        if (ec)
        {
            std::cerr << "hub " << hub << ": " << ec.message() << "\n";
            continue;
        }
        for (auto const& entry : results)
        {
            auto const ip = entry.endpoint().address();
            auto const text = ip.is_v6() ? "[" + ip.to_string() + "]:" + port
                                         : ip.to_string() + ":" + port;
            seeds.emplace_back(text, "hub " + host);
        }
    }
    for (auto const& entry : bootcache)
        seeds.emplace_back(entry.address, "bootcache");
    return seeds;
}

std::vector<PeerRecord>
survey(Args const& args, std::vector<BootcacheEntry> const& bootcache)
{
    RecordObserver observer;
    if (args.events)
    {
        observer = [](PeerRecord const& record) {
            std::cout << boost::json::serialize(to_json(record)) << "\n"
                      << std::flush;
        };
    }
    return run_survey(survey_seeds(args, bootcache), args.survey, observer);
}

/// The survey's own handshakes queue on the link and inflate its pings, so
/// ready peers are timed again a few at a time. A peer that has filled up
/// since keeps the new outcome and drops out of the ranking.
std::vector<PeerRecord>
measure(Args const& args, std::vector<PeerRecord> records)
{
    auto const ready = rank_ready(records);
    if (ready.empty() || args.measure_concurrency == 0)
        return records;
    SurveyOptions options = args.survey;
    options.follow = false;
    options.crawl = false;
    options.gossip_secs = 0;
    options.concurrency = args.measure_concurrency;
    std::vector<std::pair<std::string, std::string>> seeds;
    for (auto const& r : ready)
        seeds.emplace_back(r.address, r.source);
    std::cerr << "measuring " << ready.size() << " ready peers, "
              << args.measure_concurrency << " at a time\n";
    std::map<std::string, PeerRecord> measured;
    for (auto& m : run_survey(seeds, options, {}))
        measured.emplace(m.address, std::move(m));
    for (auto& r : records)
    {
        auto const it = measured.find(r.address);
        if (it == measured.end())
            continue;
        auto const& m = it->second;
        r.outcome = m.outcome;
        r.detail = m.detail;
        r.handshake_ms = m.handshake_ms;
        r.ping_ms = m.ping_ms;
        if (m.last_ledger)
            r.last_ledger = m.last_ledger;
    }
    return records;
}

void
save_survey(
    fs::path const& path,
    Args const& args,
    std::vector<PeerRecord> const& records)
{
    boost::json::array rows;
    for (auto const& r : records)
        rows.push_back(to_json(r));
    boost::json::object doc;
    doc["measured_at"] = now_text("%Y-%m-%dT%H:%M:%S%z");
    doc["network_id"] = args.survey.network_id;
    doc["records"] = std::move(rows);
    std::ofstream(path) << boost::json::serialize(doc) << "\n";
    std::cerr << "survey saved to " << path.string() << "\n";
}

std::vector<PeerRecord>
load_survey(fs::path const& path)
{
    std::ifstream in(path);
    std::stringstream text;
    text << in.rdbuf();
    // Bind the parsed value before walking it: a temporary would dangle.
    auto const doc = boost::json::parse(text.str());
    std::vector<PeerRecord> records;
    for (auto const& row : doc.as_object().at("records").as_array())
        records.push_back(record_from_json(row.as_object()));
    return records;
}

bool
refuse_if_open(fs::path const& db)
{
    auto const pids = open_by(db);
    if (pids.empty())
        return false;
    std::cerr << db.string() << " is open by pid";
    for (auto const& pid : pids)
        std::cerr << " " << pid;
    std::cerr << ": stop the node first\n";
    return true;
}

int
cmd_rank(Args const& args)
{
    std::vector<BootcacheEntry> bootcache;
    if (args.db)
    {
        bootcache = read_bootcache(*args.db);
        if (!args.from && !open_by(*args.db).empty())
            std::cerr
                << "warning: the node is running; its traffic shares this "
                   "link and inflates every timing\n";
    }
    auto records = args.from ? load_survey(*args.from)
                             : measure(args, survey(args, bootcache));
    annotate_geo(args, records);
    if (args.json_out)
        save_survey(*args.json_out, args, records);
    auto const ranked = rank_ready(records);
    auto const chosen = choose(ranked, args);
    std::set<std::string> marked;
    for (auto const& c : chosen)
        marked.insert(c.address);
    auto& out = args.events ? std::cerr : std::cout;
    std::vector<PeerRecord> const shown(
        ranked.begin(),
        ranked.begin() + std::min<std::size_t>(ranked.size(), args.show));
    print_table(out, shown, marked);
    out << "* = would be seeded (" << chosen.size() << " of " << ranked.size()
        << " ready peers, across " << distinct_prefixes(chosen) << " /16s)\n";
    return 0;
}

int
cmd_seed(Args const& args)
{
    if (!args.db)
        return usage();
    auto const& db = *args.db;
    if (!args.dry_run && refuse_if_open(db))
        return 1;
    auto const bootcache = read_bootcache(db);
    int64_t top = 0;
    std::set<std::string> present;
    for (auto const& entry : bootcache)
    {
        top = std::max(top, entry.valence);
        present.insert(entry.address);
    }

    auto records = args.from ? load_survey(*args.from)
                             : measure(args, survey(args, bootcache));
    annotate_geo(args, records);
    if (args.json_out)
        save_survey(*args.json_out, args, records);
    auto const chosen = choose(rank_ready(records), args);
    if (chosen.empty())
    {
        std::cerr << "no ready peers found\n";
        return 1;
    }

    std::vector<std::pair<std::string, int64_t>> plan;
    for (std::size_t i = 0; i < chosen.size(); ++i)
        plan.emplace_back(
            chosen[i].address, top + static_cast<int64_t>(chosen.size() - i));

    auto& out = args.events ? std::cerr : std::cout;
    out << db.string() << ": " << bootcache.size()
        << " entries, highest valence " << top << "\n";
    for (std::size_t i = 0; i < plan.size(); ++i)
    {
        out << std::setw(3) << i + 1 << std::setw(6) << std::fixed
            << std::setprecision(0) << *chosen[i].ping_min() << "ms  valence "
            << plan[i].second << "  " << plan[i].first << "  ("
            << (present.contains(plan[i].first) ? "update" : "insert") << ", "
            << prefix_of(plan[i].first) << ")\n";
    }
    auto const spread = distinct_prefixes(chosen);
    if (spread < 4)
        std::cerr << "warning: the seeds span only " << spread << " /16s\n";
    if (args.dry_run)
    {
        out << "dry run: nothing written\n";
        return 0;
    }

    // The survey takes minutes; the node may have started meanwhile.
    if (refuse_if_open(db))
        return 1;
    fs::create_directories(state_dir());
    auto const backup =
        state_dir() / ("peerfinder-" + now_text("%Y%m%d-%H%M%S") + ".sqlite");
    copy_database(db, backup);
    upsert_valences(db, plan);

    boost::json::array seeds;
    for (std::size_t i = 0; i < plan.size(); ++i)
    {
        seeds.push_back(boost::json::object{
            {"address", plan[i].first},
            {"ping_ms", *chosen[i].ping_min()},
            {"valence", plan[i].second}});
    }
    boost::json::object last{
        {"db", fs::absolute(db).string()},
        {"backup", backup.string()},
        {"written_at", now_text("%Y-%m-%dT%H:%M:%S%z")},
        {"seeds", std::move(seeds)}};
    std::ofstream(state_dir() / "last-seed.json")
        << boost::json::serialize(last) << "\n";
    out << "seeded " << plan.size() << " peers across " << spread
        << " /16s; backup " << backup.string() << "\n";
    return 0;
}

std::optional<boost::json::object>
read_last_seed()
{
    std::ifstream in(state_dir() / "last-seed.json");
    if (!in)
        return std::nullopt;
    std::stringstream text;
    text << in.rdbuf();
    auto const doc = boost::json::parse(text.str());
    return doc.as_object();
}

/// One JSON-RPC call to a node's admin port, which speaks plain HTTP.
boost::json::value
rpc_call(std::string const& url, std::string const& method)
{
    auto const response = http_post(
        url,
        boost::json::serialize(boost::json::object{
            {"method", method},
            {"params", boost::json::array{boost::json::object{}}}}));
    auto const doc = boost::json::parse(response.body);
    return doc.at("result");
}

int
cmd_check(Args const& args)
{
    std::map<std::string, double> seeds;
    if (auto last = read_last_seed())
    {
        for (auto const& seed : last->at("seeds").as_array())
        {
            auto const& s = seed.as_object();
            seeds[std::string(s.at("address").as_string())] =
                s.at("ping_ms").to_number<double>();
        }
    }
    auto const result = rpc_call(args.rpc, "peers");
    if (result.as_object().if_contains("status") &&
        result.at("status").as_string() != "success")
    {
        std::cerr << "peers RPC: " << boost::json::serialize(result) << "\n";
        return 1;
    }
    struct Row
    {
        std::string address;
        std::optional<int64_t> latency;
        std::string version;
    };
    std::vector<Row> rows;
    for (auto const& peer : result.at("peers").as_array())
    {
        auto const& p = peer.as_object();
        Row row;
        auto const raw = p.if_contains("address")
            ? std::string(p.at("address").as_string())
            : "";
        row.address = normalize_endpoint(raw, true).value_or(raw);
        if (auto const* latency = p.if_contains("latency");
            latency && latency->is_number())
            row.latency = latency->to_number<int64_t>();
        if (auto const* version = p.if_contains("version");
            version && version->is_string())
            row.version = std::string(version->as_string());
        rows.push_back(std::move(row));
    }
    std::stable_sort(rows.begin(), rows.end(), [](Row const& a, Row const& b) {
        return a.latency.value_or(INT64_MAX) < b.latency.value_or(INT64_MAX);
    });

    std::cout << std::setw(9) << "latency" << std::setw(6) << "seed"
              << std::setw(9) << "probed"
              << "  address\n";
    std::vector<int64_t> seeded_latency, other_latency;
    std::size_t hits = 0;
    for (auto const& row : rows)
    {
        auto const seed = seeds.find(row.address);
        bool const seeded = seed != seeds.end();
        hits += seeded;
        if (row.latency)
            (seeded ? seeded_latency : other_latency).push_back(*row.latency);
        std::cout << std::setw(9)
                  << (row.latency ? std::to_string(*row.latency) + "ms" : "?")
                  << std::setw(6) << (seeded ? "yes" : "") << std::setw(7)
                  << (seeded ? std::to_string(static_cast<int>(seed->second)) +
                              "ms"
                             : "")
                  << "  " << row.address << "  " << row.version << "\n";
    }
    std::cout << rows.size() << " peers connected; " << hits << " of "
              << seeds.size() << " seeds among them\n";
    auto median = [](std::vector<int64_t> values) {
        std::sort(values.begin(), values.end());
        return values[values.size() / 2];
    };
    if (!seeded_latency.empty())
        std::cout << "seeded: median latency " << median(seeded_latency)
                  << "ms over " << seeded_latency.size() << "\n";
    if (!other_latency.empty())
        std::cout << "other: median latency " << median(other_latency)
                  << "ms over " << other_latency.size() << "\n";
    return 0;
}

int
cmd_restore(Args const& args)
{
    if (!args.db)
        return usage();
    auto const last = read_last_seed();
    if (!last)
    {
        std::cerr << "no seed recorded in " << state_dir().string() << "\n";
        return 1;
    }
    auto const seeded = fs::path(std::string(last->at("db").as_string()));
    if (fs::weakly_canonical(seeded) != fs::weakly_canonical(*args.db))
    {
        std::cerr << "the last seed was written to " << seeded.string() << "\n";
        return 1;
    }
    if (refuse_if_open(*args.db))
        return 1;
    auto const backup = fs::path(std::string(last->at("backup").as_string()));
    copy_database(backup, *args.db);
    std::cout << "restored " << args.db->string() << " from " << backup.string()
              << "\n";
    return 0;
}

int
cmd_probe(Args args)
{
    auto endpoints = args.endpoints;
    if (endpoints.empty())
    {
        std::string line;
        while (std::getline(std::cin, line))
        {
            if (!line.empty() && line.front() != '#')
                endpoints.push_back(line);
        }
    }
    if (endpoints.empty())
        return usage();
    // Only these endpoints: don't follow what they report.
    args.survey.follow = false;
    args.survey.crawl = false;
    std::vector<std::pair<std::string, std::string>> seeds;
    for (auto const& endpoint : endpoints)
    {
        if (!normalize_endpoint(endpoint, true))
            std::cerr << "skipping " << endpoint
                      << ": not a public IP endpoint\n";
        seeds.emplace_back(endpoint, "argument");
    }
    args.survey.ipv6 = true;
    run_survey(seeds, args.survey, [](PeerRecord const& record) {
        std::cout << boost::json::serialize(to_json(record)) << "\n"
                  << std::flush;
    });
    return 0;
}

}  // namespace

int
main(int argc, char** argv)
{
    if (argc >= 2 &&
        (std::string(argv[1]) == "-h" || std::string(argv[1]) == "--help"))
    {
        usage(std::cout);
        return 0;
    }
    auto args = parse_args(argc, argv);
    if (!args)
        return usage();
    if (args->db && !fs::exists(*args->db))
    {
        std::cerr << args->db->string() << " does not exist\n";
        return 1;
    }

    Logger::set_level(LogLevel::ERROR);
    Logger::set_output_stream(&std::cerr);
    fs::create_directories(state_dir());
    catl::peer_client::node_identity::set_keys_path(
        (args->identity ? *args->identity : state_dir() / "node-key").string());

    try
    {
        if (args->command == "rank")
            return cmd_rank(*args);
        if (args->command == "seed")
            return cmd_seed(*args);
        if (args->command == "check")
            return cmd_check(*args);
        if (args->command == "restore")
            return cmd_restore(*args);
        if (args->command == "probe")
            return cmd_probe(*args);
    }
    catch (std::exception const& e)
    {
        std::cerr << "peer-seed: " << e.what() << "\n";
        return 1;
    }
    return usage();
}

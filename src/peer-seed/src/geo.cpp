#include "geo.h"

#include "http.h"

#include <chrono>
#include <fstream>
#include <iostream>
#include <set>
#include <sstream>
#include <thread>

namespace peer_seed {

namespace {

constexpr char const* kBatchUrl =
    "http://ip-api.com/batch"
    "?fields=status,message,country,countryCode,city,lat,lon,isp,org,as,query";
constexpr std::size_t kBatchSize = 100;

boost::json::object
load_cache(std::filesystem::path const& path)
{
    std::ifstream in(path);
    if (!in)
        return {};
    std::stringstream text;
    text << in.rdbuf();
    boost::system::error_code ec;
    auto const doc = boost::json::parse(text.str(), ec);
    if (ec || !doc.is_object())
        return {};
    return doc.as_object();
}

int
header_seconds(HttpResponse const& response, char const* name)
{
    auto const it = response.headers.find(name);
    if (it == response.headers.end())
        return -1;
    try
    {
        return std::stoi(it->second);
    }
    catch (std::exception const&)
    {
        return -1;
    }
}

}  // namespace

std::string
as_number(boost::json::object const& geo)
{
    auto const* as = geo.if_contains("as");
    if (!as || !as->is_string())
        return {};
    std::string text(as->as_string());
    return text.substr(0, text.find(' '));
}

std::map<std::string, boost::json::object>
geolocate(
    std::vector<std::string> const& ips,
    std::filesystem::path const& cache_path)
{
    auto cache = load_cache(cache_path);
    std::vector<std::string> missing;
    std::set<std::string> seen;
    for (auto const& ip : ips)
    {
        if (seen.insert(ip).second && !cache.contains(ip))
            missing.push_back(ip);
    }

    if (!missing.empty())
        std::cerr << "geo: looking up " << missing.size()
                  << " addresses at ip-api.com\n";
    for (std::size_t start = 0; start < missing.size(); start += kBatchSize)
    {
        boost::json::array batch;
        for (std::size_t i = start;
             i < std::min(missing.size(), start + kBatchSize);
             ++i)
            batch.emplace_back(missing[i]);
        HttpResponse response;
        for (int attempt = 0; attempt < 2; ++attempt)
        {
            try
            {
                response = http_post(kBatchUrl, boost::json::serialize(batch));
            }
            catch (std::exception const& e)
            {
                std::cerr << "geo: " << e.what() << "\n";
                response = {};
                break;
            }
            if (response.status != 429)
                break;
            auto const wait = std::max(header_seconds(response, "x-ttl"), 1);
            std::cerr << "geo: rate limited, waiting " << wait << "s\n";
            std::this_thread::sleep_for(std::chrono::seconds(wait + 1));
        }
        if (response.status != 200)
        {
            std::cerr << "geo: batch failed (HTTP " << response.status << ")\n";
            continue;
        }
        boost::system::error_code ec;
        auto const doc = boost::json::parse(response.body, ec);
        if (!ec && doc.is_array())
        {
            for (auto const& row : doc.as_array())
            {
                if (!row.is_object())
                    continue;
                auto object = row.as_object();
                auto const* status = object.if_contains("status");
                auto const* query = object.if_contains("query");
                if (!status || !query || !query->is_string() ||
                    *status != "success")
                    continue;
                std::string const ip(query->as_string());
                object.erase("status");
                object.erase("query");
                cache[ip] = std::move(object);
            }
        }
        // Spend the free tier's per-minute allowance politely.
        if (header_seconds(response, "x-rl") == 0 &&
            start + kBatchSize < missing.size())
        {
            auto const wait = std::max(header_seconds(response, "x-ttl"), 1);
            std::cerr << "geo: rate window spent, waiting " << wait << "s\n";
            std::this_thread::sleep_for(std::chrono::seconds(wait + 1));
        }
    }
    if (!missing.empty())
    {
        // The cache only saves repeat lookups; failing to write it is a note.
        std::error_code ec;
        std::filesystem::create_directories(cache_path.parent_path(), ec);
        auto const temporary =
            std::filesystem::path(cache_path.string() + ".tmp");
        std::ofstream out(temporary, std::ios::trunc);
        out << boost::json::serialize(cache) << "\n";
        out.close();
        if (out)
            std::filesystem::rename(temporary, cache_path, ec);
        if (!out || ec)
            std::cerr << "geo: couldn't save " << cache_path.string() << "\n";
    }

    std::map<std::string, boost::json::object> out;
    for (auto const& ip : seen)
    {
        if (auto const* geo = cache.if_contains(ip); geo && geo->is_object())
            out.emplace(ip, geo->as_object());
    }
    return out;
}

}  // namespace peer_seed

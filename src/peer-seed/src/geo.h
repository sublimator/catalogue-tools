#pragma once

// Geo-IP lookups through ip-api.com's batch endpoint (free, no key, HTTP
// only, 100 addresses per request, 15 requests a minute). Results are cached
// by IP in a JSON file, so a repeat survey looks up only new addresses.

#include <boost/json.hpp>

#include <filesystem>
#include <map>
#include <string>
#include <vector>

namespace peer_seed {

/// One object per IP that resolved, with ip-api's fields: country,
/// countryCode, city, lat, lon, isp, org, as.
std::map<std::string, boost::json::object>
geolocate(
    std::vector<std::string> const& ips,
    std::filesystem::path const& cache);

/// "AS16509" from ip-api's "AS16509 Amazon.com, Inc.", or empty.
std::string
as_number(boost::json::object const& geo);

}  // namespace peer_seed

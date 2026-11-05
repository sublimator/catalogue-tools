#pragma once

// A plain-HTTP POST, for a node's admin JSON-RPC port and the geo lookup.
// (catl::rpc::RpcClient always speaks TLS, so it can't be used for either.)

#include <map>
#include <string>

namespace peer_seed {

struct HttpResponse
{
    unsigned status = 0;
    std::string body;
    std::map<std::string, std::string> headers;  // names lower-cased
};

/// POST body to an http:// URL. Throws on a transport failure.
HttpResponse
http_post(
    std::string const& url,
    std::string const& body,
    int timeout_secs = 10);

}  // namespace peer_seed

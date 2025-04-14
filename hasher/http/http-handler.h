#pragma once

#include "hasher/ledger.h"
#include "http-concepts.h"

#include <memory>
#include <regex>
#include <sstream>
#include <string>

// LedgerHandler implementation
class LedgerRequestHandler : public HttpRequestHandler
{
private:
    std::shared_ptr<LedgerStore> ledgerStore_;

public:
    explicit LedgerRequestHandler(const std::shared_ptr<LedgerStore>& store)
        : ledgerStore_(store)
    {
    }

    void
    handleRequest(
        const std::string& path,
        const std::string& method,
        AbstractResponse& res) override
    {
        if (path == "/health")
        {
            handle_health(res);
        }
        else if (path.find("/ledger/") == 0)
        {
            handle_ledger(path, res);
        }
        else
        {
            // Handle 404 Not Found
            res.setStatus(404);
            res.setBody("{\"error\": \"Not found\"}");
        }
    }

private:
    void
    handle_health(AbstractResponse& res) const;
    void
    handle_ledger(const std::string& path, AbstractResponse& res) const;
};
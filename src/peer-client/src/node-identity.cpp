#include <catl/peer-client/node-identity.h>

#include <sodium.h>

#include <mutex>
#include <utility>

namespace catl::peer_client::node_identity {

namespace {

std::mutex&
mutex()
{
    static std::mutex m;
    return m;
}

std::optional<std::string>&
seed_storage()
{
    static std::optional<std::string> v;
    return v;
}

std::optional<std::string>&
path_storage()
{
    static std::optional<std::string> v;
    return v;
}

// Zero the contents of an in-place std::string before it's reassigned
// or cleared. C++17 guarantees contiguous storage so `data()` is a
// valid memzero target. sodium_memzero is volatile-safe — the compiler
// cannot elide it.
void
secure_clear(std::optional<std::string>& slot)
{
    if (slot.has_value())
    {
        auto& s = *slot;
        if (!s.empty())
        {
            ::sodium_memzero(s.data(), s.size());
        }
        slot.reset();
    }
}

}  // namespace

void
set_seed_b58(std::optional<std::string> seed)
{
    std::lock_guard lk(mutex());
    // Zero the previous seed before its storage is freed/reused.
    secure_clear(seed_storage());
    seed_storage() = std::move(seed);
}

std::optional<std::string>
seed_b58()
{
    std::lock_guard lk(mutex());
    return seed_storage();
}

void
set_keys_path(std::optional<std::string> path)
{
    std::lock_guard lk(mutex());
    path_storage() = std::move(path);
}

std::optional<std::string>
keys_path()
{
    std::lock_guard lk(mutex());
    return path_storage();
}

}  // namespace catl::peer_client::node_identity

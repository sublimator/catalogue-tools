#pragma once

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

namespace catl::peer::monitor::fmt {

inline std::string
number(uint64_t num)
{
    // Format with thousands separators using manual insertion
    // (avoids std::locale("") which makes a system call each time)
    auto s = std::to_string(num);
    int pos = static_cast<int>(s.size()) - 3;
    while (pos > 0)
    {
        s.insert(pos, ",");
        pos -= 3;
    }
    return s;
}

inline std::string
bytes(double b)
{
    const char* suffixes[] = {"B", "K", "M", "G", "T"};
    int i = 0;
    while (b > 1024 && i < 4)
    {
        b /= 1024;
        i++;
    }
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%.2f %s", b, suffixes[i]);
    return buf;
}

inline std::string
rate(double r)
{
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%.1f", r);
    return buf;
}

inline std::string
elapsed(double seconds)
{
    int total_sec = static_cast<int>(seconds);
    char buf[16];
    std::snprintf(
        buf,
        sizeof(buf),
        "%02d:%02d:%02d",
        total_sec / 3600,
        (total_sec % 3600) / 60,
        total_sec % 60);
    return buf;
}

inline std::string
spinner()
{
    static int frame = 0;
    const std::vector<std::string> frames = {
        "⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"};
    frame = (frame + 1) % frames.size();
    return frames[frame];
}

}  // namespace catl::peer::monitor::fmt

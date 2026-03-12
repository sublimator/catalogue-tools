// Standalone FTXUI rendering benchmark
// Build: cmake --build build --target ftxui-bench
// Run:   ./build/src/lesser-peer/ftxui-bench

#include <chrono>
#include <cstdio>
#include <string>
#include <vector>

#include <ftxui/component/component.hpp>
#include <ftxui/component/loop.hpp>
#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/dom/elements.hpp>
#include <ftxui/screen/color.hpp>
#include <ftxui/screen/screen.hpp>

using namespace ftxui;

// Simulate a peer card like the old peers tab
Element
render_card(int index, bool with_border, bool with_size_constraints)
{
    Elements lines;
    lines.push_back(hbox({
        text("● ") | color(Color::GreenLight),
        text("peer-" + std::to_string(index)) | bold | color(Color::GreenLight),
    }));
    lines.push_back(hbox({
        text("Addr: ") | dim,
        text("127.0.0.1:" + std::to_string(51235 + index)),
    }));
    lines.push_back(hbox({
        text("State: ") | dim,
        text("Connected") | color(Color::GreenLight),
    }));
    lines.push_back(hbox({
        text("Ver: ") | dim,
        text("rippled-2.4.0") | dim,
        text("  Proto: ") | dim,
        text("2.3") | dim,
    }));
    lines.push_back(hbox({
        text("Uptime: ") | dim,
        text("01:23:45") | bold,
    }));
    lines.push_back(hbox({
        text("Last pkt: ") | dim,
        text("0s"),
    }));
    lines.push_back(hbox({
        text("Pkts: ") | dim,
        text("12,345") | bold,
        text("  Bytes: ") | dim,
        text("1.23 MB") | bold,
    }));
    lines.push_back(hbox({
        text("Rate: ") | dim,
        text("45.2/s") | color(Color::GreenLight),
        text("  ") | dim,
        text("12.3 KB/s") | color(Color::Cyan),
    }));

    // Packet type lines (22 types, split into 2 columns)
    static const std::vector<std::string> types = {
        "mtPING",
        "mtMANIFESTS",
        "mtCLUSTER",
        "mtENDPOINTS",
        "mtTRANSACTION",
        "mtGET_LEDGER",
        "mtLEDGER_DATA",
        "mtPROPOSE_LEDGER",
        "mtSTATUS_CHANGE",
        "mtHAVE_SET",
        "mtVALIDATION",
        "mtGET_OBJECTS",
        "mtVALIDATORLIST",
        "mtSQUELCH",
        "mtVALIDATORLISTCOLLECTION",
        "mtPROOF_PATH_REQ",
        "mtPROOF_PATH_RESPONSE",
        "mtREPLAY_DELTA_REQ",
        "mtREPLAY_DELTA_RESPONSE",
        "mtHAVE_TRANSACTIONS",
        "mtTRANSACTIONS",
        "mtPEER_SHARD_INFO_V2",
    };

    Elements left_col, right_col;
    for (size_t i = 0; i < types.size(); ++i)
    {
        auto line = hbox({
            text("  ") | dim,
            text(types[i]) | color(Color::Yellow) | size(WIDTH, EQUAL, 28),
            text(": ") | dim,
            text(std::to_string(i * 100)) | dim | size(WIDTH, EQUAL, 6),
        });
        if (i % 2 == 0)
            left_col.push_back(line);
        else
            right_col.push_back(line);
    }
    lines.push_back(text("Packet types:") | dim);
    lines.push_back(hbox({
        vbox(left_col) | size(WIDTH, EQUAL, 42),
        separator(),
        vbox(right_col) | size(WIDTH, EQUAL, 42),
    }));

    auto card = vbox(lines);
    if (with_border)
        card = card | border;
    return card;
}

// Same card but with pre-formatted text (no per-type hbox)
Element
render_card_simple(int index)
{
    Elements lines;
    lines.push_back(hbox({
        text("● ") | color(Color::GreenLight),
        text("peer-" + std::to_string(index)) | bold | color(Color::GreenLight),
    }));
    lines.push_back(text("Addr: 127.0.0.1:" + std::to_string(51235 + index)) |
                     dim);
    lines.push_back(
        text("State: Connected  Uptime: 01:23:45  Last: 0s") | dim);
    lines.push_back(
        text("Pkts: 12,345  Bytes: 1.23 MB  Rate: 45.2/s 12.3 KB/s") | dim);

    // Packet types as plain text
    static const std::vector<std::string> types = {
        "mtPING",         "mtMANIFESTS",    "mtCLUSTER",
        "mtENDPOINTS",    "mtTRANSACTION",  "mtGET_LEDGER",
        "mtLEDGER_DATA",  "mtVALIDATION",   "mtSTATUS_CHANGE",
        "mtHAVE_SET",     "mtPROPOSE_LEDGER"};
    for (auto const& t : types)
    {
        char buf[64];
        std::snprintf(buf, sizeof(buf), "  %-26s %6d", t.c_str(), 42);
        lines.push_back(text(buf) | dim);
    }

    return vbox(lines) | border;
}

// Table row (like the new peers tab)
Element
render_table_row(int index)
{
    return hbox({
        text("● ") | color(Color::GreenLight) | size(WIDTH, EQUAL, 3),
        text("peer-" + std::to_string(index)) | bold | color(Color::GreenLight) |
            size(WIDTH, EQUAL, 10),
        text("Connected") | color(Color::GreenLight) | size(WIDTH, EQUAL, 14),
        text("127.0.0.1:" + std::to_string(51235 + index)) |
            size(WIDTH, EQUAL, 22),
        text("01:23:45") | size(WIDTH, EQUAL, 10),
        text("0s") | size(WIDTH, EQUAL, 6),
        text("12,345") | bold | size(WIDTH, EQUAL, 10),
        text("1.23 MB") | size(WIDTH, EQUAL, 10),
        text("45.2/s") | color(Color::GreenLight) | size(WIDTH, EQUAL, 8),
        text("12.3 KB/s") | color(Color::Cyan) | size(WIDTH, EQUAL, 10),
        text("rippled-2.4.0") | dim,
    });
}

struct BenchResult
{
    std::string name;
    int iterations;
    double total_us;
    double avg_us;
};

template <typename Fn>
BenchResult
bench(std::string name, int iterations, Fn fn)
{
    // Warmup
    for (int i = 0; i < 3; ++i)
        fn();

    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < iterations; ++i)
        fn();
    auto elapsed = std::chrono::steady_clock::now() - start;
    double total_us =
        std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();

    return {name, iterations, total_us, total_us / iterations};
}

int
main()
{
    const int N = 100;  // iterations per benchmark
    const int peers = 20;
    const int cards = 9;

    std::printf("FTXUI Rendering Benchmark\n");
    std::printf("=========================\n");
    std::printf("Peers: %d, Cards: %d, Iterations: %d\n\n", peers, cards, N);

    std::vector<BenchResult> results;

    // 1. Card construction only (no border, no size)
    results.push_back(bench(
        "9 cards (no border, no size)", N, [&] {
            Elements cards_el;
            for (int i = 0; i < cards; ++i)
                cards_el.push_back(render_card(i, false, false));
            return vbox(cards_el);
        }));

    // 2. Card construction with border
    results.push_back(bench(
        "9 cards (border, no size)", N, [&] {
            Elements cards_el;
            for (int i = 0; i < cards; ++i)
                cards_el.push_back(render_card(i, true, false));
            return vbox(cards_el);
        }));

    // 3. Card construction with border + size constraints in grid
    results.push_back(bench(
        "9 cards (border + grid size)", N, [&] {
            Elements cards_el;
            for (int i = 0; i < cards; ++i)
                cards_el.push_back(render_card(i, true, true));

            const int columns = 3;
            std::vector<Element> rows;
            for (size_t i = 0; i < cards_el.size(); i += columns)
            {
                Elements row;
                for (int c = 0; c < columns; ++c)
                {
                    size_t idx = i + c;
                    if (idx < cards_el.size())
                        row.push_back(
                            cards_el[idx] | size(WIDTH, EQUAL, 120) |
                            size(HEIGHT, EQUAL, 30));
                    else
                        row.push_back(
                            filler() | size(WIDTH, EQUAL, 120) |
                            size(HEIGHT, EQUAL, 30));
                    if (c < columns - 1)
                        row.push_back(separator());
                }
                rows.push_back(hbox(row));
            }
            return vbox(rows);
        }));

    // 4. Simple cards (pre-formatted text)
    results.push_back(bench(
        "9 simple cards (text only)", N, [&] {
            Elements cards_el;
            for (int i = 0; i < cards; ++i)
                cards_el.push_back(render_card_simple(i));
            return vbox(cards_el);
        }));

    // 5. Table rows (new layout)
    results.push_back(bench(
        "20 table rows", N, [&] {
            Elements rows;
            for (int i = 0; i < peers; ++i)
                rows.push_back(render_table_row(i));
            return vbox(rows);
        }));

    // 6. Element construction + Screen::ToString (full render pipeline)
    results.push_back(bench(
        "9 cards + Screen render", N, [&] {
            Elements cards_el;
            for (int i = 0; i < cards; ++i)
                cards_el.push_back(render_card(i, true, true));

            const int columns = 3;
            std::vector<Element> rows;
            for (size_t i = 0; i < cards_el.size(); i += columns)
            {
                Elements row;
                for (int c = 0; c < columns; ++c)
                {
                    size_t idx = i + c;
                    if (idx < cards_el.size())
                        row.push_back(
                            cards_el[idx] | size(WIDTH, EQUAL, 120) |
                            size(HEIGHT, EQUAL, 30));
                    else
                        row.push_back(
                            filler() | size(WIDTH, EQUAL, 120) |
                            size(HEIGHT, EQUAL, 30));
                    if (c < columns - 1)
                        row.push_back(separator());
                }
                rows.push_back(hbox(row));
            }
            auto doc = vbox(rows);
            auto screen = Screen::Create(Dimension::Fixed(360),
                                         Dimension::Fixed(90));
            Render(screen, doc);
            return screen.ToString();
        }));

    // 7. Table rows + Screen render
    results.push_back(bench(
        "20 rows + Screen render", N, [&] {
            Elements rows;
            for (int i = 0; i < peers; ++i)
                rows.push_back(render_table_row(i));
            auto doc = vbox(rows);
            auto screen = Screen::Create(Dimension::Fixed(360),
                                         Dimension::Fixed(90));
            Render(screen, doc);
            return screen.ToString();
        }));

    // Print results
    std::printf("%-35s %8s %12s %10s\n", "Benchmark", "Iters", "Total(ms)",
                "Avg(us)");
    std::printf("%-35s %8s %12s %10s\n", "---", "---", "---", "---");
    for (auto const& r : results)
    {
        std::printf(
            "%-35s %8d %12.1f %10.1f\n",
            r.name.c_str(),
            r.iterations,
            r.total_us / 1000.0,
            r.avg_us);
    }

    return 0;
}

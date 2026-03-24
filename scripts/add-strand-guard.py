#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.14"
# dependencies = ["tree-sitter>=0.24", "tree-sitter-cpp>=0.23"]
# ///
"""
Add ENSURE_ON_STRAND() auto-repost guards to all PeerSet methods that
touch internal state. Uses tree-sitter to find method bodies.

Usage:
    scripts/add-strand-guard.py src/peer-client/src/peer-set.cpp
    scripts/add-strand-guard.py src/peer-client/src/peer-set.cpp --dry-run
"""

import sys
from pathlib import Path

import tree_sitter_cpp as tscpp
from tree_sitter import Language, Parser


CPP = Language(tscpp.language())
parser = Parser(CPP)

# Methods that need the guard (touch PeerSet state).
# Methods with args need manual capture — we generate the full repost.
GUARD_METHODS = {
    "note_discovered",
    "note_status",
    "note_connect_success",
    "note_connect_failure",
    "update_endpoint_stats",
    "queue_connect",
    "queue_crawl",
    "pump_connects",
    "pump_crawls",
    "sort_pending_connects",
    "sort_pending_crawls",
    "bootstrap",
    "try_undiscovered",
    "start_tracked_endpoints",
    "start_connect",
    "prioritize_ledger",
    "try_candidates_for",
    "remove_peer",
    "peer_for",
    "any_peer",
    "evict_for",
    "at_connection_cap",
    "connected_count",
    "should_connect_endpoint",
    "endpoint_has_range",
    "endpoint_covers_preferred_ledger",
    "candidate_better",
}

# Skip these — they have co_await post(strand_) at the top already
SKIP_METHODS = {
    "wait_for_peer",
    "wait_for_any_peer",
    "try_connect",
    "co_size",
}

GUARD_LINE = '    if (!strand_.running_in_this_thread()) {{ asio::post(strand_, [self = shared_from_this(){captures}]() {{ self->{call}; }}); return{ret}; }}'
ASSERT_LINE = '    ASSERT_ON_STRAND();'


def find_peerset_methods(source: bytes):
    """Find all PeerSet:: method definitions and their body locations."""
    tree = parser.parse(source)
    results = []

    for node in walk(tree.root_node):
        if node.type != "function_definition":
            continue

        # Get the declarator to find the method name
        declarator = node.child_by_field_name("declarator")
        if declarator is None:
            continue

        # Look for PeerSet:: qualified name
        text = source[declarator.start_byte:declarator.end_byte].decode("utf8", errors="replace")
        if "PeerSet::" not in text:
            continue

        # Extract method name
        name = None
        for part in text.split("PeerSet::"):
            if part:
                name = part.split("(")[0].strip()
                break

        if name is None:
            continue

        # Find the compound_statement (body)
        body = node.child_by_field_name("body")
        if body is None:
            continue

        # Get parameters
        params = []
        param_node = declarator
        # Walk to find parameter_list
        for child in walk(declarator):
            if child.type == "parameter_list":
                for p in child.children:
                    if p.type == "parameter_declaration":
                        # Get the parameter name (last identifier in the declaration)
                        pname = None
                        for pc in walk(p):
                            if pc.type == "identifier":
                                pname = source[pc.start_byte:pc.end_byte].decode("utf8")
                        if pname:
                            params.append(pname)
                break

        # Check return type
        ret_type = ""
        type_node = node.child_by_field_name("type")
        if type_node:
            ret_type = source[type_node.start_byte:type_node.end_byte].decode("utf8").strip()

        # Body start (first line after opening brace)
        body_start_line = body.start_point.row  # 0-indexed, the { line

        results.append({
            "name": name,
            "params": params,
            "ret_type": ret_type,
            "body_start_line": body_start_line,
            "body_start_byte": body.start_byte,
            "is_const": "const" in text.split(")")[-1] if ")" in text else False,
        })

    return results


def walk(node):
    """Walk all nodes in the tree."""
    yield node
    for child in node.children:
        yield from walk(child)


def build_guard(method):
    """Build the appropriate guard line for a method."""
    name = method["name"]
    params = method["params"]
    ret_type = method["ret_type"]

    if name in SKIP_METHODS:
        return None

    if name not in GUARD_METHODS:
        return None

    # For const methods, just assert (can't shared_from_this in const context easily)
    if method["is_const"]:
        return ASSERT_LINE

    # Build captures and call
    captures = ""
    if params:
        captures = ", " + ", ".join(params)

    args = ", ".join(params)
    call = f"{name}({args})"

    # Return value for non-void
    ret = ""
    if ret_type and ret_type != "void":
        ret = " {}"  # default-construct return

    return GUARD_LINE.format(captures=captures, call=call, ret=ret)


def process_file(path: Path, dry_run: bool = False):
    source = path.read_bytes()
    lines = source.decode("utf8").split("\n")
    methods = find_peerset_methods(source)

    # Sort by line number descending so insertions don't shift later lines
    methods.sort(key=lambda m: m["body_start_line"], reverse=True)

    changes = 0
    for method in methods:
        guard = build_guard(method)
        if guard is None:
            continue

        insert_line = method["body_start_line"] + 1  # after the {

        # Check if guard already exists
        if insert_line < len(lines):
            existing = lines[insert_line].strip()
            if "ASSERT_ON_STRAND" in existing or "ENSURE_ON_STRAND" in existing or "strand_.running_in_this_thread" in existing:
                # Remove existing guard
                lines.pop(insert_line)

        lines.insert(insert_line, guard)
        changes += 1
        print(f"  {method['name']}() line {method['body_start_line'] + 1}: {'ASSERT' if 'ASSERT' in guard else 'REPOST'}")

    if changes == 0:
        print("No changes needed.")
        return

    print(f"\n{changes} methods guarded.")

    if not dry_run:
        path.write_text("\n".join(lines))
        print(f"Written to {path}")
    else:
        print("(dry run — no changes written)")


def main():
    args = sys.argv[1:]
    dry_run = "--dry-run" in args
    args = [a for a in args if a != "--dry-run"]

    if not args:
        print(f"Usage: {sys.argv[0]} <file.cpp> [--dry-run]")
        return 1

    path = Path(args[0])
    if not path.exists():
        print(f"File not found: {path}")
        return 1

    process_file(path, dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

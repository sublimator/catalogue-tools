#!/usr/bin/env python3
"""
Find camelCase variables in C++ source files with editor integration

This script scans C++ source files for variable names that use camelCase naming
convention (starting with lowercase, contains uppercase, not snake_case).

It maintains a todo list of findings that can be navigated through keyboard shortcuts,
opening each item in your preferred editor.

Usage:
    python find_camel_case.py [path] [options]

If path is not provided, it defaults to 'src'

Options:
    --editor=EDITOR          Specify the editor command to use (default: clion1)
    --editor-template=TPL    Template for editor command (default: '{filename}:{line}:{column}')
                             Only used for non-JetBrains editors
                             Supports: {filename}, {line}, {row_0_based}, {col_0_based}, {column}
    --keybinding=KEY         Global keyboard shortcut to trigger next item (default: cmd+shift+f12)
                             Format: e.g., 'cmd+shift+f12' or 'ctrl+alt+n'
    --load-only              Load existing todo list without rescanning
"""

import os
import re
import sys
import argparse
from pathlib import Path
import concurrent.futures
import subprocess
import json
import tempfile
from threading import Thread
import time

# Detect platform
import platform

PLATFORM = platform.system()
IS_MACOS = PLATFORM == "Darwin"
IS_WINDOWS = PLATFORM == "Windows"
IS_LINUX = PLATFORM == "Linux"

# Try to import pynput for keyboard support
try:
    import pynput
    from pynput import keyboard
    from pynput.keyboard import Key, HotKey

    PYNPUT_AVAILABLE = True
except ImportError:
    PYNPUT_AVAILABLE = False
    print(
        "Warning: 'pynput' package not found. Install with 'pip install pynput' for keybinding support."
    )
    if IS_MACOS:
        print(
            "On macOS, you'll need to grant Accessibility permissions to Terminal/Python"
        )
        print("in System Preferences → Security & Privacy → Privacy → Accessibility")
    elif IS_LINUX:
        print("On Linux, you may need to run with sudo for proper keyboard access")

# Regular expressions for camelCase detection
# Looks for variables that start with lowercase and contain uppercase
# Main pattern for variables: handles Type varName patterns
VAR_PATTERN = re.compile(
    r"(^|\s+)([a-zA-Z0-9_:]+(?:<[^>]+>)?(?:::)?[a-zA-Z0-9_:]+)\s+([a-z][a-z0-9_]*[A-Z][a-zA-Z0-9_]*)\s*[;=({]"
)

# Additional pattern for member variables with this->camelCase
THIS_PATTERN = re.compile(r"this->([a-z][a-z0-9_]*[A-Z][a-zA-Z0-9_]*)")

# Pattern for class members: captures className::methodName() { ... varName = ... }
CLASS_METHOD_VAR_PATTERN = re.compile(
    r"([a-zA-Z0-9_]+)::([a-zA-Z0-9_]+)\s*\([^)]*\)\s*\{[^}]*\b([a-z][a-z0-9_]*[A-Z][a-zA-Z0-9_]*)\b"
)

# Filter out common false positives
FALSE_POSITIVE_PATTERNS = [
    re.compile(r"\breturn\b"),
    re.compile(r"\bthrow\b"),
    re.compile(r"\bif\b"),
    re.compile(r"\belse\b"),
    re.compile(r"\bfor\b"),
    re.compile(r"\bwhile\b"),
    re.compile(r"\bnamespace\b"),
    re.compile(r"\btypedef\b"),
    re.compile(r"\btemplate\b"),
    re.compile(r"#include"),
    re.compile(r"\bcatch\b"),
    re.compile(r"\bclass\b"),
    re.compile(r"\busing\b"),
    re.compile(r"\benum\b"),
    re.compile(r"\bstruct\b"),
]

# List of standard container types to check for in angle brackets
CONTAINER_TYPES = [
    "vector",
    "map",
    "unordered_map",
    "set",
    "unordered_set",
    "list",
    "deque",
    "queue",
    "stack",
    "array",
    "tuple",
    "pair",
    "optional",
    "variant",
]

# Common standard library typedefs that might contain uppercase chars
STD_TYPEDEFS = [
    "string",
    "wstring",
    "u8string",
    "u16string",
    "u32string",
    "stringstream",
    "istringstream",
    "ostringstream",
    "wstringstream",
    "iostream",
    "istream",
    "ostream",
    "ifstream",
    "ofstream",
    "size_t",
    "ptrdiff_t",
    "int8_t",
    "int16_t",
    "int32_t",
    "int64_t",
    "uint8_t",
    "uint16_t",
    "uint32_t",
    "uint64_t",
    "uintptr_t",
    "intptr_t",
]

# Skip files or directories containing these substrings
SKIP_PATHS = [
    "/build/",
    "/deps/",
    "/lib/",
    "/third_party/",
    "/external/",
    "/vendor/",
    "/test/",
]

# Global todo list for findings
TODO_LIST = []
CURRENT_INDEX = 0
TODO_FILE = None
HOTKEY_LISTENER = None


def is_cpp_file(file_path):
    """Check if a file is a C++ source or header file."""
    extensions = {".cpp", ".cc", ".cxx", ".c", ".hpp", ".h", ".hxx", ".inl"}
    return file_path.suffix.lower() in extensions


def is_false_positive(line):
    """Check if a line is likely a false positive."""
    for pattern in FALSE_POSITIVE_PATTERNS:
        if pattern.search(line):
            return True
    return False


def is_template_parameter(var_name, line):
    """Check if the variable might be a template parameter."""
    template_pattern = re.compile(r"template\s*<.*\b" + re.escape(var_name) + r"\b.*>")
    return template_pattern.search(line)


def process_file(file_path):
    """Process a single file and return camelCase findings."""
    results = []
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()

            # Process the file line by line
            for i, line in enumerate(lines):
                line_num = i + 1

                # Find potential column position of the variable within the line
                def find_var_position(var_name, line_text):
                    col = line_text.find(var_name)
                    return col if col >= 0 else 0

                # Skip preprocessor directives and false positives
                if line.strip().startswith("#") or is_false_positive(line):
                    continue

                # Find variable declarations using camelCase
                for match in VAR_PATTERN.finditer(line):
                    var_type, var_name = match.group(2), match.group(3)

                    # Check if it's a std type (not a variable)
                    if var_name in STD_TYPEDEFS or any(
                        container in var_type for container in CONTAINER_TYPES
                    ):
                        continue

                    # Check if it's a template parameter
                    if is_template_parameter(var_name, line):
                        continue

                    col_position = find_var_position(var_name, line)
                    results.append((line_num, line.strip(), var_name, col_position))

                # Find 'this->camelCase' usage
                for match in THIS_PATTERN.finditer(line):
                    var_name = match.group(1)
                    col_position = find_var_position(var_name, line)
                    results.append((line_num, line.strip(), var_name, col_position))

                # Find local variables in method implementations
                for match in CLASS_METHOD_VAR_PATTERN.finditer(
                    "\n".join(lines[max(0, i - 10) : min(len(lines), i + 10)])
                ):
                    class_name, method_name, var_name = (
                        match.group(1),
                        match.group(2),
                        match.group(3),
                    )
                    if (
                        i - 10 <= match.start() <= i + 10
                    ):  # Ensure the match is near the current line
                        col_position = find_var_position(var_name, line)
                        results.append(
                            (
                                line_num,
                                f"In {class_name}::{method_name}: {line.strip()}",
                                var_name,
                                col_position,
                            )
                        )
    except Exception as e:
        print(f"Error processing {file_path}: {e}", file=sys.stderr)

    return file_path, results


def find_camel_case_variables(base_dir):
    """Find all camelCase variables in C++ files under the given directory."""
    all_results = {}
    files_to_process = []

    # Find all C++ files
    for root, dirs, files in os.walk(base_dir):
        # Skip specified directories
        dirs[:] = [
            d
            for d in dirs
            if not any(skip in os.path.join(root, d) for skip in SKIP_PATHS)
        ]

        path = Path(root)
        for file in files:
            file_path = path / file
            if is_cpp_file(file_path) and not any(
                skip in str(file_path) for skip in SKIP_PATHS
            ):
                files_to_process.append(file_path)

    print(f"Processing {len(files_to_process)} files...")

    # Process files in parallel
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for file_path, results in executor.map(process_file, files_to_process):
            if results:
                all_results[file_path] = results

    return all_results


def build_todo_list(results):
    """Build a todo list from the results."""
    global TODO_LIST
    TODO_LIST = []

    for file_path, findings in sorted(results.items()):
        if findings:
            for line_num, line_text, var_name, col_position in findings:
                TODO_LIST.append(
                    {
                        "file_path": str(file_path),
                        "line_num": line_num,
                        "column": col_position,
                        "row_0_based": line_num - 1,
                        "col_0_based": col_position,
                        "var_name": var_name,
                        "line_text": line_text,
                    }
                )

    # Save todo list to temp file for persistence
    save_todo_list()

    return TODO_LIST


def save_todo_list():
    """Save todo list to a temporary file."""
    global TODO_FILE

    if TODO_FILE is None:
        # Create a temporary file that persists after the program exits
        temp_dir = tempfile.gettempdir()
        TODO_FILE = os.path.join(temp_dir, "camelcase_todo.json")

    with open(TODO_FILE, "w") as f:
        json.dump({"current_index": CURRENT_INDEX, "items": TODO_LIST}, f)

    print(f"Todo list saved to {TODO_FILE}")


def load_todo_list():
    """Load todo list from temporary file if it exists."""
    global TODO_LIST, CURRENT_INDEX, TODO_FILE

    temp_dir = tempfile.gettempdir()
    TODO_FILE = os.path.join(temp_dir, "camelcase_todo.json")

    if os.path.exists(TODO_FILE):
        try:
            with open(TODO_FILE, "r") as f:
                data = json.load(f)
                TODO_LIST = data.get("items", [])
                CURRENT_INDEX = data.get("current_index", 0)
            print(f"Loaded todo list with {len(TODO_LIST)} items from {TODO_FILE}")
            print(f"Current position: {CURRENT_INDEX + 1}/{len(TODO_LIST)}")
            return True
        except Exception as e:
            print(f"Error loading todo list: {e}")

    return False


def open_in_editor(item, editor_cmd, editor_template):
    """Open the file in the specified editor at the given line and column."""
    if not item:
        print("No item to open.")
        return False

    filename = item["file_path"]
    line = item["line_num"]
    column = item["column"]
    row_0_based = item["row_0_based"]
    col_0_based = item["col_0_based"]

    # Check if it's an IntelliJ/CLion editor based on command name
    is_intellij_editor = any(
        editor in editor_cmd.lower()
        for editor in [
            "intellij",
            "idea",
            "clion",
            "pycharm",
            "webstorm",
            "phpstorm",
            "clion1",
        ]
    )

    if is_intellij_editor:
        # IntelliJ/CLion uses --line and --column arguments
        # Example: clion --line 42 --column 3 /path/to/file.cpp
        cmd_template = f"--line {line} --column {column} {filename}"
        full_cmd = f"{editor_cmd} {cmd_template}"
    else:
        # Apply the user's custom template for other editors
        cmd_template = editor_template.format(
            filename=filename,
            line=line,
            column=column,
            row_0_based=row_0_based,
            col_0_based=col_0_based,
        )
        full_cmd = f"{editor_cmd} {cmd_template}"

    print(f"Opening: {full_cmd}")
    try:
        subprocess.Popen(full_cmd, shell=True)
        return True
    except Exception as e:
        print(f"Error opening editor: {e}")
        return False


def display_current_item():
    """Display the current item in the todo list."""
    global TODO_LIST, CURRENT_INDEX

    if not TODO_LIST:
        print("Todo list is empty.")
        return

    if CURRENT_INDEX < 0 or CURRENT_INDEX >= len(TODO_LIST):
        CURRENT_INDEX = 0

    item = TODO_LIST[CURRENT_INDEX]
    print(f"\nItem {CURRENT_INDEX + 1}/{len(TODO_LIST)}:")
    print(f"File: {item['file_path']}")
    print(f"Line {item['line_num']}, Column {item['column']}: {item['var_name']}")
    print(f"  {item['line_text']}")


def next_item(editor_cmd, editor_template):
    """Move to the next item in the todo list and open it in the editor."""
    global CURRENT_INDEX

    if not TODO_LIST:
        print("Todo list is empty.")
        return

    CURRENT_INDEX = (CURRENT_INDEX + 1) % len(TODO_LIST)
    save_todo_list()

    display_current_item()
    open_in_editor(TODO_LIST[CURRENT_INDEX], editor_cmd, editor_template)


def prev_item(editor_cmd, editor_template):
    """Move to the previous item in the todo list and open it in the editor."""
    global CURRENT_INDEX

    if not TODO_LIST:
        print("Todo list is empty.")
        return

    CURRENT_INDEX = (CURRENT_INDEX - 1) % len(TODO_LIST)
    save_todo_list()

    display_current_item()
    open_in_editor(TODO_LIST[CURRENT_INDEX], editor_cmd, editor_template)


def next_item_with_feedback(editor_cmd, editor_template):
    """Wrapper for next_item that provides feedback to the user."""
    print("\n--- Next Item (via hotkey) ---")
    next_item(editor_cmd, editor_template)


def prev_item_with_feedback(editor_cmd, editor_template):
    """Wrapper for prev_item that provides feedback to the user."""
    print("\n--- Previous Item (via hotkey) ---")
    prev_item(editor_cmd, editor_template)


def parse_hotkey(hotkey_str):
    """Parse a hotkey string (e.g., 'cmd+shift+f12') into pynput keys."""
    keys = []
    parts = hotkey_str.split("+")
    for part in parts:
        part = part.strip().lower()
        if part == "ctrl" or part == "control":
            keys.append(Key.ctrl)
        elif part == "alt" or part == "option":
            keys.append(Key.alt)
        elif part == "shift":
            keys.append(Key.shift)
        elif part == "cmd" or part == "command":
            keys.append(Key.cmd)
        elif part.startswith("f") and part[1:].isdigit():
            # Function keys F1-F20
            fn_num = int(part[1:])
            key_name = f"f{fn_num}"
            if hasattr(Key, key_name):
                keys.append(getattr(Key, key_name))
        else:
            # Regular key
            if len(part) == 1:  # Single character
                keys.append(part)
            else:
                # Try to find a matching key in pynput.keyboard.Key
                key_name = part.lower()
                if hasattr(Key, key_name):
                    keys.append(getattr(Key, key_name))
    return keys


def format_key_combo(keys):
    """Format a list of pynput keys as a readable string."""
    key_names = []
    for k in keys:
        if isinstance(k, str):
            key_names.append(k)
        else:
            name = str(k).replace("Key.", "")
            if name == "cmd":
                # Use more familiar name on macOS
                name = "command"
            key_names.append(name)
    return "+".join(key_names)


def setup_hotkeys(keybinding, editor_cmd, editor_template):
    """Set up global hotkeys using pynput."""
    if not PYNPUT_AVAILABLE:
        print("\nHotkey support not available. Install pynput package:")
        print("    pip install pynput")
        if IS_MACOS:
            print(
                "\nOn macOS, you'll need to grant Accessibility permissions to Terminal/Python"
            )
            print(
                "in System Preferences → Security & Privacy → Privacy → Accessibility"
            )
        return False

    try:
        # Parse the keybinding string into pynput format
        main_keys = parse_hotkey(keybinding)

        # Create a previous hotkey - use f11 instead of f12 if applicable, or add alt
        prev_keys = None
        if "f12" in keybinding.lower():
            prev_keys = parse_hotkey(keybinding.lower().replace("f12", "f11"))
        else:
            # Try to create a different but related key combo for previous
            if Key.alt not in main_keys:
                prev_keys = main_keys + [Key.alt]
            else:
                # Use a completely different key combination
                prev_keys = parse_hotkey("ctrl+shift+p")

        # Create hotkeys for next and previous items
        start_hotkey_listener(
            [
                (
                    main_keys,
                    lambda: next_item_with_feedback(editor_cmd, editor_template),
                ),
                (
                    prev_keys,
                    lambda: prev_item_with_feedback(editor_cmd, editor_template),
                ),
            ]
        )

        # Display keybinding info
        print("\nGlobal hotkeys are now active:")
        main_key_str = format_key_combo(main_keys)
        prev_key_str = format_key_combo(prev_keys)
        print(f"- Press {main_key_str} to move to the next item")
        print(f"- Press {prev_key_str} to move to the previous item")
        print("- Press Ctrl+C to exit the program")

        return True
    except Exception as e:
        print(f"Error setting up hotkeys: {e}")
        return False


def start_hotkey_listener(hotkey_bindings):
    """Start a pynput keyboard listener for hotkeys.

    Args:
        hotkey_bindings: List of tuples (keys, callback)
    """
    global HOTKEY_LISTENER

    # Create hotkey objects
    hotkeys = []
    for keys, callback in hotkey_bindings:
        hotkeys.append(HotKey(keys, callback))

    # Create a function to check all hotkeys
    def on_press(key):
        for hotkey in hotkeys:
            hotkey.press(key)

    def on_release(key):
        for hotkey in hotkeys:
            hotkey.release(key)

    # Start listener in a separate thread
    HOTKEY_LISTENER = keyboard.Listener(on_press=on_press, on_release=on_release)
    HOTKEY_LISTENER.daemon = True
    HOTKEY_LISTENER.start()


def keyboard_listener():
    """Keep the script running to detect keyboard events."""
    try:
        print("Press Ctrl+C to exit the program.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nExiting...")
        sys.exit(0)


def display_results(results):
    """Display the results in a readable format."""
    total_vars = 0

    for file_path, findings in sorted(results.items()):
        if findings:
            print(f"\n{file_path}:")
            print("-" * 80)
            for line_num, line_text, var_name, _ in findings:
                print(f"  Line {line_num}: {var_name}")
                print(f"    {line_text}")
            total_vars += len(findings)

    print("\n" + "=" * 80)
    print(f"Found {total_vars} camelCase variables in {len(results)} files")


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Find camelCase variables in C++ source files with editor integration"
    )
    parser.add_argument(
        "path", nargs="?", default="src", help="Directory to scan (default: src)"
    )
    parser.add_argument(
        "--editor", default="clion1", help="Editor command to use (default: clion1)"
    )
    parser.add_argument(
        "--editor-template",
        default="{filename}:{line}:{column}",
        help="Template for editor command (default: {filename}:{line}:{column})",
    )
    parser.add_argument(
        "--keybinding",
        default="cmd+shift+f12",
        help="Global keyboard shortcut to trigger next item (default: cmd+shift+f12)",
    )
    parser.add_argument(
        "--load-only",
        action="store_true",
        help="Load existing todo list without rescanning",
    )
    return parser.parse_args()


def interactive_mode(editor_cmd, editor_template):
    """Run an interactive console for navigating the todo list."""
    print("\nInteractive Mode:")
    print("  n: Next item")
    print("  p: Previous item")
    print("  c: Show current item")
    print("  g <num>: Go to item number")
    print("  d <num>: Delete item number (remove from todo list)")
    print("  s: Save current progress")
    print("  r: Refresh todo list (reload from file)")
    print("  k: Show current hotkeys")
    print("  q: Quit")

    while True:
        cmd = input("\nCommand (n/p/c/g/d/s/r/k/q): ").strip().lower()

        if cmd == "q":
            break
        elif cmd == "n":
            next_item(editor_cmd, editor_template)
        elif cmd == "p":
            prev_item(editor_cmd, editor_template)
        elif cmd == "c":
            display_current_item()
        elif cmd == "s":
            save_todo_list()
            print("Todo list saved.")
        elif cmd == "r":
            if load_todo_list():
                display_current_item()
            else:
                print("Could not reload todo list.")
        elif cmd == "k":
            if PYNPUT_AVAILABLE:
                print("\nKeyboard Shortcuts:")
                print(f"  Global: {args.keybinding} - Next item")
                if "f12" in args.keybinding.lower():
                    prev_key = args.keybinding.lower().replace("f12", "f11")
                    print(f"  Global: {prev_key} - Previous item")
                print("  Interactive mode: n, p, c, g, d, s, r, q")
            else:
                print("\nKeyboard shortcuts not available. Install pynput:")
                print("    pip install pynput")
        elif cmd.startswith("g "):
            try:
                index = int(cmd.split(" ")[1]) - 1
                if 0 <= index < len(TODO_LIST):
                    global CURRENT_INDEX
                    CURRENT_INDEX = index
                    save_todo_list()
                    display_current_item()
                    open_in_editor(
                        TODO_LIST[CURRENT_INDEX], editor_cmd, editor_template
                    )
                else:
                    print(f"Index out of range. Valid range: 1-{len(TODO_LIST)}")
            except (ValueError, IndexError):
                print("Invalid index number")
        elif cmd.startswith("d "):
            try:
                index = int(cmd.split(" ")[1]) - 1
                if 0 <= index < len(TODO_LIST):
                    item = TODO_LIST.pop(index)
                    print(
                        f"Removed item {index+1}: {item['var_name']} in {item['file_path']}"
                    )

                    # Adjust current index if needed
                    if CURRENT_INDEX >= len(TODO_LIST):
                        CURRENT_INDEX = max(0, len(TODO_LIST) - 1)
                    elif CURRENT_INDEX > index:
                        CURRENT_INDEX -= 1

                    save_todo_list()
                    if TODO_LIST:
                        display_current_item()
                    else:
                        print("Todo list is now empty.")
                else:
                    print(f"Index out of range. Valid range: 1-{len(TODO_LIST)}")
            except (ValueError, IndexError):
                print("Invalid index number")
        else:
            print("Unknown command")


def main():
    """Main function."""
    global args
    args = parse_arguments()
    base_dir = args.path
    editor_cmd = args.editor
    editor_template = args.editor_template
    keybinding = args.keybinding
    load_only = args.load_only

    # Try to load existing todo list
    if load_only:
        if load_todo_list():
            # Successfully loaded existing todo list
            pass
        else:
            print(
                "No existing todo list found. Use without --load-only to scan for new items."
            )
            return 1
    else:
        if not os.path.isdir(base_dir):
            print(f"Error: {base_dir} is not a valid directory", file=sys.stderr)
            return 1

        print(f"Scanning {base_dir} for camelCase variables...")
        results = find_camel_case_variables(base_dir)
        display_results(results)

        # Build todo list from results
        build_todo_list(results)

    if not TODO_LIST:
        print("No camelCase variables found or todo list is empty.")
        return 0

    # Display the first item
    display_current_item()

    # Check if pynput is available
    if not PYNPUT_AVAILABLE:
        print("\nNOTE: For keyboard shortcuts, install pynput:")
        print("    pip install pynput")
        if IS_MACOS:
            print(
                "\nOn macOS, you'll need to grant Accessibility permissions to Terminal/Python"
            )
            print(
                "in System Preferences → Security & Privacy → Privacy → Accessibility"
            )
        elif IS_LINUX:
            print("\nOn Linux, you may need to run with sudo for keyboard access")
    else:
        # Set up hotkeys
        setup_hotkeys(keybinding, editor_cmd, editor_template)

        # Start keyboard listener in a separate thread
        listener_thread = Thread(target=keyboard_listener)
        listener_thread.daemon = True
        listener_thread.start()

    # Open the first item in the editor
    open_in_editor(TODO_LIST[CURRENT_INDEX], editor_cmd, editor_template)

    # Start interactive mode
    interactive_mode(editor_cmd, editor_template)

    return 0


if __name__ == "__main__":
    sys.exit(main())

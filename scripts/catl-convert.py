#!/usr/bin/env python3
"""
Convert CATL v1 files to CATL v2 format with optional compression.
"""

import argparse
import json
import logging
import os
import subprocess
import sys
from pathlib import Path


def setup_logging(level):
    """Set up logging configuration."""
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=log_format,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)


def get_repo_root():
    """Get the repository root directory."""
    try:
        result = subprocess.run(
            ['git', 'rev-parse', '--show-toplevel'],
            capture_output=True,
            text=True,
            check=True
        )
        return Path(result.stdout.strip())
    except subprocess.CalledProcessError:
        return None


def load_config_file(config_path):
    """Load configuration from JSON file if it exists."""
    if not config_path.exists():
        return {}
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
            # Expand ~ in all paths
            for preset in config.values():
                if 'input' in preset:
                    preset['input'] = os.path.expanduser(preset['input'])
                if 'output' in preset:
                    preset['output'] = os.path.expanduser(preset['output'])
            return config
    except (json.JSONDecodeError, IOError) as e:
        logging.warning(f"Failed to load config file: {e}")
        return {}


def run_command(cmd, logger, check=True, capture=False):
    """Run a command and optionally check for errors."""
    logger.debug(f"Running: {' '.join(cmd)}")
    
    if capture:
        result = subprocess.run(cmd, capture_output=True, text=True)
    else:
        # Let output stream directly to terminal
        result = subprocess.run(cmd, text=True)
    
    if check and result.returncode != 0:
        if capture and result.stderr:
            logger.error(f"Command failed: {result.stderr}")
        sys.exit(result.returncode)
    
    return result


def main():
    parser = argparse.ArgumentParser(description='Convert CATL v1 to CATL v2 format')
    
    # Config selection
    parser.add_argument('--config',
                        help='Use a preset configuration from .catl-convert.local.json')
    parser.add_argument('--list-configs',
                        action='store_true',
                        help='List available configurations and exit')
    
    # File paths
    parser.add_argument('--input',
                        help='Input CATL v1 file')
    parser.add_argument('--output',
                        help='Output CATL v2 file')
    
    # Build options
    parser.add_argument('--build-dir', 
                        default=os.environ.get('BUILD_DIR', 'build'),
                        help='Build directory (default: build or $BUILD_DIR)')
    parser.add_argument('--skip-build',
                        action='store_true',
                        help='Skip the build step')
    
    # Conversion options
    parser.add_argument('--skip-convert',
                        action='store_true',
                        help='Skip conversion, just compress existing output file')
    parser.add_argument('--max-ledgers',
                        type=int,
                        default=0,
                        help='Maximum number of ledgers to process (0 = all)')
    
    # Verification options
    parser.add_argument('--skip-probe',
                        action='store_true',
                        help='Skip the verification probe after conversion')
    parser.add_argument('--probe-ledger',
                        help='Ledger number to probe for verification')
    parser.add_argument('--probe-key',
                        help='Key to probe for verification')
    
    # Compression options
    parser.add_argument('--compress',
                        action='store_true',
                        help='Compress output with zstd (creates .zst file)')
    parser.add_argument('--compress-level',
                        type=int,
                        default=3,
                        choices=range(1, 23),
                        metavar='[1-22]',
                        help='Zstd compression level 1-22 (default: 3)')
    
    # Logging
    parser.add_argument('--log-level',
                        default=os.environ.get('LOG_LEVEL', 'info'),
                        choices=['debug', 'info', 'warning', 'error'],
                        help='Log level (default: info)')
    
    # Extra arguments
    parser.add_argument('--extra-args',
                        nargs='*',
                        default=[],
                        help='Extra arguments to pass to the converter')
    
    args = parser.parse_args()
    
    # Set up logging
    logger = setup_logging(args.log_level)
    
    # Load configuration file if it exists
    config_presets = {}
    repo_root = get_repo_root()
    if repo_root:
        config_file = repo_root / '.catl-convert.local.json'
        if config_file.exists():
            logger.info(f"Loading config from {config_file}")
            config_presets = load_config_file(config_file)
    
    # Handle --list-configs
    if args.list_configs:
        if not config_presets:
            print("No configurations found. Create .catl-convert.local.json in repository root.")
            sys.exit(0)
        
        print("Available configurations:")
        for name, config in config_presets.items():
            print(f"\n{name}:")
            if 'input' in config:
                print(f"  Input:  {config['input']}")
            if 'output' in config:
                print(f"  Output: {config['output']}")
            if 'probe_ledger' in config:
                print(f"  Probe ledger: {config['probe_ledger']}")
            if 'max_ledgers' in config and config['max_ledgers'] > 0:
                print(f"  Max ledgers: {config['max_ledgers']}")
            if 'extra_args' in config:
                print(f"  Extra args: {' '.join(config['extra_args'])}")
        sys.exit(0)
    
    # Load preset if specified
    preset = {}
    if args.config:
        if args.config not in config_presets:
            logger.error(f"Configuration '{args.config}' not found in .catl-convert.local.json")
            sys.exit(1)
        preset = config_presets[args.config]
        logger.info(f"Using configuration: {args.config}")
    
    # Apply configuration with command-line overrides
    if not args.input:
        args.input = preset.get('input')
    if args.input:
        args.input = os.path.expanduser(args.input)
    
    if not args.output:
        args.output = preset.get('output')
    if args.output:
        args.output = os.path.expanduser(args.output)
    
    # Check required arguments
    if not args.skip_convert and (not args.input or not args.output):
        logger.error("--input and --output are required unless --skip-convert is used")
        sys.exit(1)
    
    if args.compress and not args.output:
        logger.error("--output is required when using --compress")
        sys.exit(1)
    
    # Apply other preset values if not specified
    if not args.probe_ledger:
        args.probe_ledger = preset.get('probe_ledger')
    if not args.probe_key:
        args.probe_key = preset.get('probe_key')
    if args.max_ledgers == 0:
        args.max_ledgers = preset.get('max_ledgers', 0)
    if preset.get('skip_probe'):
        args.skip_probe = True
    
    # Combine extra args
    preset_extra_args = preset.get('extra_args', [])
    all_extra_args = preset_extra_args + args.extra_args
    
    # Paths
    build_dir = Path(args.build_dir)
    converter = build_dir / 'src' / 'utils-v2' / 'catl1-to-catl2'
    
    # Build
    if not args.skip_build:
        logger.info("Building project...")
        # Configure
        cmake_cmd = f'cd {build_dir} && cmake ..'
        result = subprocess.run(cmake_cmd, shell=True, text=True)
        if result.returncode != 0:
            logger.error("CMake configuration failed")
            sys.exit(result.returncode)
        
        # Build
        run_command(['ninja', '-C', str(build_dir)], logger)
        logger.info("Build complete")
    
    # Convert
    if not args.skip_convert:
        logger.info(f"Converting {args.input} -> {args.output}")
        convert_cmd = [
            str(converter),
            '--input', args.input,
            '--output', args.output,
            '--log-level', args.log_level
        ]
        
        # Add max-ledgers if specified
        if args.max_ledgers > 0:
            convert_cmd.extend(['--max-ledgers', str(args.max_ledgers)])
            logger.info(f"Processing maximum {args.max_ledgers} ledgers")
        
        # Add any extra arguments
        if all_extra_args:
            convert_cmd.extend(all_extra_args)
            logger.debug(f"Extra args: {' '.join(all_extra_args)}")
        
        run_command(convert_cmd, logger)
        logger.info("Conversion complete")
    else:
        logger.info(f"Skipping conversion, using existing: {args.output}")
    
    # Probe to verify
    if not args.skip_probe and not args.skip_convert:
        if args.probe_ledger and args.probe_key:
            logger.info("Verifying conversion with probe...")
            logger.debug(f"Probing ledger {args.probe_ledger} with key {args.probe_key[:16]}...")
            
            probe_cmd = [
                str(converter),
                '--input', args.output,
                '--get-ledger', args.probe_ledger,
                '--get-key', args.probe_key
            ]
            
            # Add extra args to probe as well
            if all_extra_args:
                probe_cmd.extend(all_extra_args)
            
            result = run_command(probe_cmd, logger, check=False)
            
            if result.returncode == 0:
                logger.info("✓ Verification successful")
            else:
                logger.warning(f"Verification probe failed with code {result.returncode}")
        else:
            logger.debug("Skipping probe - no probe_ledger or probe_key specified")
    
    # Compress if requested
    if args.compress:
        compressed_output = f"{args.output}.zst"
        logger.info(f"Compressing with zstd level {args.compress_level}...")
        
        # Build zstd command
        compress_cmd = [
            'zstd',
            f'-{args.compress_level}',  # Compression level
            '-T0',      # Use all available CPU threads
            '-f',       # Force overwrite
            '-v',       # Verbose to show progress
        ]
        
        # Only add ultra/long flags for high compression levels
        if args.compress_level >= 20:
            compress_cmd.append('--ultra')
        if args.compress_level >= 10:
            compress_cmd.append('--long')
        
        compress_cmd.extend([
            args.output,
            '-o', compressed_output
        ])
        
        # Let zstd output stream to terminal
        result = run_command(compress_cmd, logger, check=True)
        
        # Get file sizes for reporting
        original_size = Path(args.output).stat().st_size
        compressed_size = Path(compressed_output).stat().st_size
        ratio = (1 - compressed_size / original_size) * 100
        
        logger.info(f"✓ Compressed: {compressed_output}")
        logger.info(f"  Original:   {original_size:,} bytes")
        logger.info(f"  Compressed: {compressed_size:,} bytes")
        logger.info(f"  Ratio:      {ratio:.1f}% reduction")


if __name__ == '__main__':
    main()
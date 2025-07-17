#!/usr/bin/env node

/**
 * Decode XRPL binary transaction data
 * Usage: node decode-xrpl-binary.js <hex_string>
 * 
 * Install dependencies first:
 * npm install -g xrpl
 */

const { decode } = require('xrpl');

// Check command line arguments
if (process.argv.length !== 3) {
    console.error('Usage: node decode-xrpl-binary.js <hex_string>');
    console.error('Example: node decode-xrpl-binary.js 1100791503E8...');
    process.exit(1);
}

const hexString = process.argv[2];

// Validate hex string
if (!/^[0-9A-Fa-f]+$/.test(hexString)) {
    console.error('Error: Invalid hex string');
    process.exit(1);
}

try {
    // Decode the binary data
    const decoded = decode(hexString);
    
    // Pretty print the result
    console.log(JSON.stringify(decoded, null, 2));
} catch (error) {
    console.error('Error decoding binary data:', error.message);
    process.exit(1);
}
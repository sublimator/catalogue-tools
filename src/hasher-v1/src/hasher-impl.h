#pragma once

// Common settings (same in all modes)
#define COLLAPSE_STATE_MAP 0      // Disable full state map collapse (expensive)
#define STORE_LEDGER_SNAPSHOTS 1  // Enable ledger snapshots
#define STORE_LEDGER_SNAPSHOTS_EVERY 1  // Store every ledger
#define THROW_ON_TX_HASH_MISMATCH 1     // Throw on transaction hash mismatch
#define THROW_ON_AS_HASH_MISMATCH 1     // Throw on account state hash mismatch

// Mode-specific settings
#ifdef HASHER_DEVELOP_MODE     // Development mode
#define DEBUG_LEDGER_TX 81920  // Enable debugging for ledger 81920
#define STOP_AT_LEDGER 10000   // Stop processing at ledger 10000
#else                          // Production mode
#define DEBUG_LEDGER_TX 0      // Disable ledger debugging
#define STOP_AT_LEDGER 0       // Process all ledgers
#endif

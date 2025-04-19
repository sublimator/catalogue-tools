#pragma once

#define DEVELOP_MODE 1

// Defaults
#define DEBUG_LEDGER_TX 0  // Set to 0 to disable
#define COLLAPSE_STATE_MAP \
    0  // Does a full collapse of the state map which is expensive
#define STORE_LEDGER_SNAPSHOTS 1
#define STORE_LEDGER_SNAPSHOTS_EVERY 1
#define STOP_AT_LEDGER 0
#define THROW_ON_TX_HASH_MISMATCH 1
#define THROW_ON_AS_HASH_MISMATCH 1

// Development mode OVERRIDES
#if DEVELOP_MODE  // TODO: set by cmake or something
#define STORE_LEDGER_SNAPSHOTS 1
#define DEBUG_LEDGER_TX 81920
#endif
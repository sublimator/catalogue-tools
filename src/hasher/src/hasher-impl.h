#pragma once

// Set to 0 to disable
#define DEBUG_LEDGER_TX 81920
#define COLLAPSE_STATE_MAP \
    0  // Does a full collapse of the state map which is expensive
#define STORE_LEDGER_SNAPSHOTS 0
#define STORE_LEDGER_SNAPSHOTS_EVERY 1
#define STOP_AT_LEDGER 0
#define THROW_ON_TX_HASH_MISMATCH 1
#define THROW_ON_AS_HASH_MISMATCH 1

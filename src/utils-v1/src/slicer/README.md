# `catl1-slice`: High-Performance CATL v1 File Slicing Tool

**`catl1-slice` is a command-line utility designed to efficiently extract a contiguous range of ledgers (a "slice") from a larger CATL (Catalogue) v1 file. It supports both compressed (zlib) and uncompressed input and output CATL files.**

While `catl1-slice` capably fulfills its primary objective of creating individual ledger slices, its development and usage have also proven highly instructive in understanding the inherent characteristics and limitations of the CATL v1 file format, particularly concerning random access and bulk data processing.

## Core Features

* **Targeted Ledger Extraction:** Precisely extracts ledgers within a user-specified start and end sequence number.
* **Format Preservation:** Generates valid CATL v1 output files, adjusting only the file header's `min_ledger` and `max_ledger` fields to reflect the slice's range.
* **State Handling:**
  * The first ledger in the output slice contains a full account state map.
  * Subsequent ledgers in the output slice contain state map deltas, efficiently copied from the input.
* **Transaction Map Copying:** Transaction maps are byte-naively copied for each ledger in the slice.
* **Compression Support:** Handles zlib-compressed or uncompressed input and can produce compressed or uncompressed output slices and state snapshots.
* **State Snapshotting:**
  * Can utilize pre-existing full state snapshots to accelerate the process of finding the starting state for a slice, though fundamentally limited by the need to still decompress the entire stream sequentially up to that point.
  * Can create a new full state snapshot for the ledger immediately following the end of the current slice, offering theoretical optimization for consecutive slices but hampered by the underlying sequential access limitations.

## Command-Line Interface (CLI)

```
catl1-slice --input <input_catl_file> \
           --output <output_slice_file> \
           --start-ledger <start_sequence_number> \
           --end-ledger <end_sequence_number> \
           [--snapshots-path <path_to_directory_for_snapshots>] \
           [--compression-level <0-9>] \
           [--force-overwrite] \
           [--create-next-slice-state-snapshot] \
           [--no-use-start-snapshot] \
           [--log-level <error|warn|info|debug>] \
           [--help]
```

**Key CLI Arguments:**

* `--input`: Path to the source CATL v1 file.
* `--output`: Path for the generated CATL slice file.
* `--start-ledger`: Sequence number of the first ledger in the slice.
* `--end-ledger`: Sequence number of the last ledger in the slice.
* `--snapshots-path`: Directory for storing and retrieving state snapshots (e.g., `state_snapshot_for_ledger_<SEQ_NUM>.dat.zst`).
* `--compression-level`: Zlib compression level (0-9) for the output slice and any created snapshots. Default for output: 0 (uncompressed).
* `--force-overwrite`: Overwrite existing output/snapshot files without prompting.
* `--create-next-slice-state-snapshot`: (Default: true) Create a snapshot for `end_ledger + 1`. Use `--no-create-next-slice-state-snapshot` to disable.
* `--no-use-start-snapshot`: (Default: try to use start snapshot) If present, ignore existing start snapshots and always process from the input file.
* `--log-level`: Set logging verbosity (error, warn, info, debug). Default: info.

## Usage Example

```bash
./catl1-slice --input large_archive.catl \
             --output week1_slice.catl \
             --start-ledger 1000 \
             --end-ledger 1700 \
             --snapshots-path ./my_snapshots \
             --compression-level 6 \
             --create-next-slice-state-snapshot
```
This command extracts ledgers 1000 through 1700 from `large_archive.catl` into `week1_slice.catl`, using/creating snapshots in `./my_snapshots`, compressing the output at level 6, and creating a snapshot for ledger 1701.

## How It Works

The CATL slicer follows a sequential processing algorithm constrained by the nature of the CATL v1 format. Below is the pseudocode representation of the core slicing algorithm:

```
function SliceCATLFile(inputFile, outputFile, startLedger, endLedger):
    // Read and validate file header
    header = ReadCATLHeader(inputFile)
    ValidateLedgerRange(header, startLedger, endLedger)
    
    // Initialize output
    writer = CreateOutputFile(outputFile, startLedger, endLedger)
    
    // Check for available state snapshot for the start ledger
    stateMap = new StateMap()
    snapshotFile = LookupSnapshot(startLedger)
    usingSnapshot = false
    
    // Phase 1: Sequential scan until start ledger
    // PERFORMANCE BOTTLENECK: Must process entire stream up to startLedger
    currentLedger = header.minLedger
    while currentLedger < startLedger:
        ledgerInfo = ReadLedgerInfo(inputFile)
        currentLedger = ledgerInfo.sequence
        
        if currentLedger < startLedger:
            if not usingSnapshot:
                // Process state map to build current state
                ReadAndUpdateStateMap(inputFile, stateMap)
            else:
                // Skip state map since we'll use snapshot
                SkipMap(inputFile, STATE_MAP_TYPE)
            
            // Skip transaction map (not needed for state tracking)
            SkipMap(inputFile, TRANSACTION_MAP_TYPE)
    
    // Phase 2: Copy slice data to output
    // Enable "tee" functionality to copy all data read to output
    EnableTeeFunctionality(inputFile, writer)
    
    // Handle first ledger with special processing
    if usingSnapshot:
        // Copy state snapshot directly to output instead of from input file
        DisableTee(inputFile)
        CopySnapshotToOutput(snapshotFile, writer)
        SkipMap(inputFile, STATE_MAP_TYPE)  // Skip in input but don't copy
        EnableTee(inputFile)
    
    // Process ledgers in requested slice range
    ledgersProcessed = 0
    while currentLedger <= endLedger:
        ledgerInfo = ReadLedgerInfo(inputFile)  // This is tee'd to output
        currentLedger = ledgerInfo.sequence
        
        if currentLedger <= endLedger:
            // Process state map (with tee enabled, data is copied to output)
            if createNextSliceSnapshot:
                // Track state changes for snapshot creation
                ReadAndUpdateStateMap(inputFile, stateMap)
            else:
                // Just tee the state map without processing
                SkipMap(inputFile, STATE_MAP_TYPE)
            
            // Process transaction map (just tee it)
            SkipMap(inputFile, TRANSACTION_MAP_TYPE)
            
            ledgersProcessed++
    
    // Phase 3: Create state snapshot for next slice if requested
    if createNextSliceSnapshot:
        nextLedger = endLedger + 1
        DisableTee(inputFile)
        
        if nextLedger <= header.maxLedger:
            // Read next ledger to update state map
            ledgerInfo = ReadLedgerInfo(inputFile)
            ReadAndUpdateStateMap(inputFile, stateMap)
            SkipMap(inputFile, TRANSACTION_MAP_TYPE)
            
            // Create snapshot with final state
            CreateStateSnapshot(stateMap, GetSnapshotPath(nextLedger))
    
    FinalizeCATLFile(writer)
    return ledgersProcessed
```

This algorithm highlights several key limitations:

1. **Sequential Processing Requirement**: The stream-based nature of CATL v1 forces processing ledgers in sequence from the file start until the slice end point.

2. **Decompression Overhead**: For compressed files, the entire data stream must be decompressed sequentially, creating a major performance bottleneck.

3. **State Tracking**: To ensure the first ledger in the slice has complete state information, the algorithm must track all state changes from the file's beginning, unless a pre-existing snapshot is available.

4. **Inefficient Bulk Operations**: Creating multiple non-consecutive slices requires reprocessing the file from the beginning for each slice operation.

The complexity of this algorithm scales linearly with the number of ledgers to process, but the performance bottleneck is primarily in the initial phase where all ledgers up to the start ledger must be sequentially processed.

## Future Work

`catl1-slice` will likely remain in the repository as a frozen artifact of curiosity—a testament to the exploration of the CATL v1 format's limitations and the valuable lessons learned from its implementation.

Any future development efforts on v1 would be better directed toward creating an entirely new tool, perhaps a `catl-bulk-slicer` but that should be done "as needed":
* Process the input file only once to create multiple slice outputs in a single pass
* Take advantage of the knowledge gained from this initial exploration

### Just move on bro

Alternatively, and perhaps more sensibly, efforts could focus on defining a CATL v2 format specification that addresses the fundamental limitations encountered with the v1 format.

## An Instructive Tool: Exposing CATL v1 Format Limitations

The development of `catl1-slice`, particularly its focus on efficient data extraction and state management through snapshots, has been instrumental in highlighting several characteristics of the CATL v1 format:

1. **Sequential Nature:** CATL v1 is inherently a stream-oriented format. When compressed (a common scenario), accessing data for a specific ledger (`N`) necessitates decompressing the entire file body from the beginning up to that point. Even when uncompressed, true random access isn't possible; the tool must still sequentially read and skip preceding ledger data. This fundamental limitation means that performance degrades catastrophically with larger files, regardless of optimization attempts like snapshotting, since the underlying stream must still be processed sequentially.

2. **Challenges with Bulk Operations:**
   * `catl1-slice` is designed for single-slice extraction per invocation. While effective for this, it's not optimized for "bulk splitting" a single large file into numerous smaller slices in one pass. Such an operation would involve redundant processing of the input file's initial segments for each slice generated.
   * This underscores a limitation of the CATL v1 format itself: without an internal index or block structure, efficiently partitioning the file requires re-processing or more complex tooling.

3. **Snapshotting as a Workaround for Sequential Access:** The snapshot feature, which was intended to dramatically speed up the creation of *consecutive* slices, is itself a testament to the underlying need to mitigate the cost of "fast-forwarding" through the sequential CATL v1 data. In practice, however, this approach proved largely ineffective for very large files since the underlying requirement to decompress the entire stream sequentially still creates a performance bottleneck that snapshots cannot overcome. It essentially attempts to create an external index for ledger states, but cannot escape the fundamental sequential nature of the format.

4. **Performance Implications of Whole-Body Compression:** Compressing the entire CATL body, while potentially good for overall file size, inherently prevents parallel decompression of a single file and makes random data access without full prior decompression impossible.

**Conclusion on Design and Learning:**

`catl1-slice` appears to meet its design goal of extracting specific ledger ranges from CATL v1 files, employing techniques like direct data copying ("teeing") and state snapshotting. However, the tool's design was ultimately ill-considered when evaluated against performance requirements for large files. For very large CATL archives, the tool's performance becomes impractical because even with snapshotting, it must still decompress the entire stream sequentially up to the point of extraction.

The process of building this tool, however, served as a valuable exercise in practical systems design, revealing how a file format's fundamental architecture (like CATL v1's sequential, stream-based nature) profoundly influences the capabilities and optimal design of tools that interact with it. The main value derived has been the instructive experience in understanding these system-level interactions and file format limitations.

While the tool itself might seem "ill-considered" if the primary goal was, for example, to enable true random access or highly parallelizable bulk processing of CATL v1 data (tasks for which the format itself is not well-suited), its existence provides a concrete way to work with data subsets and offers deep insights into the trade-offs of simpler file format designs. This underscores the importance of considering access patterns and performance characteristics when designing file formats for large datasets.


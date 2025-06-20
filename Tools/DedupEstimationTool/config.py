from collections import defaultdict

def init():
    global fingerprints
    global files_skipped
    global bytes_total
    global files_scanned
    global chunk_count
    global last_offsets
    global zero_chunks_skipped
    global zero_bytes_skipped
    global last_offsets_real
    global last_offsets_zero

    last_offsets = defaultdict(int)
    fingerprints = set()
    files_skipped = 0
    bytes_total = 0
    files_scanned = 0
    chunk_count = 0

    zero_chunks_skipped = 0
    zero_bytes_skipped = 0
    last_offsets_zero = defaultdict(int)
    last_offsets_real = defaultdict(int)

def init():
    global fingerprints
    global files_skipped
    global bytes_total
    global files_scanned
    global chunk_count
    fingerprints = set()
    files_skipped = 0
    bytes_total = 0
    files_scanned = 0
    chunk_count = 0

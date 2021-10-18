def init():
    global fingerprints
    global files_skipped
    global bytes_total
    global files_scanned
    fingerprints = set()
    files_skipped = 0
    bytes_total = 0
    files_scanned = 0

import hashlib


seen_digests = set()
seen_fingerprints = []
near_threshold = 3


#creates a hash of the token counts
def compute_content_digest(token_counts):
    if token_counts is None or len(token_counts) == 0:
        return hashlib.sha256(b"empty").hexdigest()
    pairs = tuple(sorted(token_counts.items()))
    raw = str(pairs).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()

#creates a bit vector of a string
def string_to_bit_vector(s, size=64):
    raw_digest = hashlib.sha256(s.encode("utf-8")).digest()
    out = []
    n = 0
    for b in raw_digest:
        if n >= size:
            break
        for shift in range(7, -1, -1):
            if n >= size:
                break
            out.append((b >> shift) & 1)
            n += 1
    return out[:size]

#creates a fingerprint for a set of tokens using simhash
def page_fingerprint(token_counts, size=64):
    if not token_counts:
        return 0
    accum = [0] * size
    for word, weight in token_counts.items():
        vec = string_to_bit_vector(word, size)
        for idx in range(size):
            accum[idx] += weight if vec[idx] else -weight
    result = 0
    pos = 0
    while pos < size:
        if accum[pos] > 0:
            result |= (1 << (size - 1 - pos))
        pos += 1
    return result

#gets the similarity between two fingerprints
def count_bit_differences(fp_a, fp_b, size=64):
    diff = fp_a ^ fp_b
    mask = (1 << size) - 1
    diff &= mask
    return bin(diff).count("1")

#self explanatory, checks if the page is a duplicate
def check_duplicate(token_counts):
    global seen_digests, seen_fingerprints
    if not token_counts:
        return True
    digest = compute_content_digest(token_counts)
    if digest in seen_digests:
        return True
    fingerprint = page_fingerprint(token_counts)
    for stored in seen_fingerprints:
        if count_bit_differences(fingerprint, stored) <= near_threshold:
            return True
    seen_digests.add(digest)
    seen_fingerprints.append(fingerprint)
    return False

#restore the cache so we remember the pages we have seen
def restore_state(digest_list, fingerprint_list):
    global seen_digests, seen_fingerprints
    seen_digests |= set(digest_list or [])
    seen_fingerprints.extend(fingerprint_list or [])

#get the state of the cache so we can save it
def get_state_for_save():
    return (list(seen_digests), list(seen_fingerprints))

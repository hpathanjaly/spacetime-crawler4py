import sys

# Time Complexity: O(N), N is the number of characters in the file
# Space Complexity: O(1) - constant space, processes one token at a time
# Reads line by line and yields tokens one at a time as a generator
# which is memory-efficient as it does not store all tokens in RAM
# this makes it better for processing very large files
def tokenize(filepath):    
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                cur_token = []
                for char in line:
                    if char.isalnum() and char.isascii(): # alphanumeric chars
                        cur_token.append(char)
                    else:
                        if cur_token:
                            yield "".join(cur_token).lower()
                            cur_token = []
                # last token in line
                if cur_token:
                    yield "".join(cur_token).lower()
    # exceptions
    except Exception as e:
        print(f"Error reading file: {e}")
        sys.exit(1)

# Time Complexity: O(N), N is the number of tokens
# Space Complexity: O(V), V is the number of unique tokens for the frequency dictionary
# Counts token occurrences from an iterable (works with generators for memory efficiency)
def computeWordFrequencies(tokens):
    counts = {}
    for token in tokens:
        if token in counts:
            counts[token] += 1
        else:
            counts[token] = 1
    return counts

# Time Complexity: O(N log N) where N is the number of unique tokens.
# Prints frequencies sorted by count desc, therefore it is O(N log N) where N is the number of unique tokens
# time complexity is limited by the sorting algorithm used
def print_frequencies(frequencies):
    # sort by count and token for stability
    sorted_items = sorted(frequencies.items(), key=lambda item: (-item[1], item[0]))
    
    for token, count in sorted_items:
        print(f"{token}\t{count}")

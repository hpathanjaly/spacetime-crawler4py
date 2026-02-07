import sys
STOPWORDS = {
    "a","about","above","after","again","against","all","am","an","and","any","are",
    "aren't","as","at","be","because","been","before","being","below","between","both",
    "but","by","can't","cannot","could","couldn't","did","didn't","do","does","doesn't",
    "doing","don't","down","during","each","few","for","from","further","had","hadn't",
    "has","hasn't","have","haven't","having","he","he'd","he'll","he's","her","here",
    "here's","hers","herself","him","himself","his","how","how's","i","i'd","i'll",
    "i'm","i've","if","in","into","is","isn't","it","it's","its","itself","let's","me",
    "more","most","mustn't","my","myself","no","nor","not","of","off","on","once","only",
    "or","other","ought","our","ours","ourselves","out","over","own","same","shan't",
    "she","she'd","she'll","she's","should","shouldn't","so","some","such","than","that",
    "that's","the","their","theirs","them","themselves","then","there","there's","these",
    "they","they'd","they'll","they're","they've","this","those","through","to","too",
    "under","until","up","very","was","wasn't","we","we'd","we'll","we're","we've",
    "were","weren't","what","what's","when","when's","where","where's","which","while",
    "who","who's","whom","why","why's","with","won't","would","wouldn't","you","you'd",
    "you'll","you're","you've","your","yours","yourself","yourselves"
}

# Time Complexity: O(N), N is the number of characters in the file
# Space Complexity: O(1) - constant space, processes one token at a time
# Reads line by line and yields tokens one at a time as a generator
# which is memory-efficient as it does not store all tokens in RAM
# this makes it better for processing very large files
def tokenize(text: str): 
    tokens = {}
    for word in text.split():
        if word in STOPWORDS:
            continue
        cur_token = ""
        for char in word + " ":
            if char.isalnum() and char.isascii(): # alphanumeric chars
                cur_token += char
            else:
                if cur_token and cur_token not in STOPWORDS:
                    tokens[cur_token] = 1 + tokens.get(cur_token, 0)
                cur_token = ""
    return tokens

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

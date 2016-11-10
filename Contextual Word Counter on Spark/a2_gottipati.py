# _*_ coding:utf-8 _*_
import re
from os import path

base_dir = '/Users/bobby/Downloads'
# filenames = [
#     'tmp.py',
#     'client.js'
# ]
filenames = ['CC-MAIN-20160924173739-00000-ip-10-143-35-109.ec2.internal.warc.wet.gz']
filenames = [ path.join(base_dir, fn) for fn in filenames]
combined_path = ','.join(filenames)

rdd = sc.textFile(combined_path)
counts = rdd.map(lambda s: s.lower())\
            .flatMap(extractWordsRegex)\
            .reduceByKey(lambda x,y: x+y)\
            .sortBy(lambda x: x[1], ascending=False)
sample = rdd.sample(False, 0.0001)

s = 'i feel good i am i feel feel i feel i feel hi feel good hi feel. i feel, good'
su = 'é'
# 10268093 in 1st file

def getWordEnd(line, word_start):
    word_end = line.find(" ", word_start)
    if word_end == -1:
        word_end = len(line)
    else:
        # discount the space
        word_end = word_end - 1
    return line[word_start : word_end + 1]

def extractWords(line):
    counts = {}
    feel_start_str = "i feel "
    feel_str = " i feel "
    remaining_idx = 0
    search_str = None
    
    while remaining_idx < len(line):
        if not remaining_idx and line.startswith(feel_start_str):
            search_str = feel_start_str
        else:
            search_str = feel_str

        next_word_start = line.find(search_str, remaining_idx)
        if next_word_start == -1:
            break

        next_word_start += len(search_str)
        next_word = getWordEnd(line, next_word_start)
        # minus 1 to include the space before the word
        remaining_idx = next_word_start - 1
        try:
            next_word.encode('ascii', errors='strict')
        except:
            continue
        stripped_word = []
        i, j = 0, len(next_word)
        while not next_word[i].isalnum():
            i += 1
        while not next_word[j].isalnum():
            j -= 1
        next_word = next_word[i:j+1]
        #next_word.strip(r',./:;\'"|[]{}-_?+=!@#$%^&*()')
        if next_word:
            counts[next_word] = counts.get(next_word, 0) + 1
        
    return counts.items()

def extractWordsRegex(line):
    counts = {}
    next_words = re.findall(r'(?<=\bi feel\s)(\w+)', line)
    for next_word in next_words:
        if not next_word:
            continue
        try:
            next_word.encode('ascii', errors='strict')
            counts[next_word] = counts.get(next_word, 0) + 1
        except:
            continue

    return counts.items()




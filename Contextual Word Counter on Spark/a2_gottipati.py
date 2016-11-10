# _*_ coding:utf-8 _*_
import re
from os import path

feel_words_regex = re.compile(r'(?<=\bi feel\s)(\w+)')
def extractWordsRegex(line):
    counts = {}
    line = line.lower()
    next_words = re.findall(feel_words_regex, line)
    for next_word in next_words:
        if not next_word:
            continue
        try:
            next_word.encode('ascii', errors='strict')
            counts[next_word] = counts.get(next_word, 0) + 1
        except:
            continue

    return counts.items()

base_dir = '/Users/bobby/Downloads'
filenames = [
    'CC-MAIN-20160924173739-00000-ip-10-143-35-109.ec2.internal.warc.wet.gz',
    'CC-MAIN-20160924173739-00001-ip-10-143-35-109.ec2.internal.warc.wet.gz',
    'CC-MAIN-20160924173739-00002-ip-10-143-35-109.ec2.internal.warc.wet.gz',
    'CC-MAIN-20160924173739-00003-ip-10-143-35-109.ec2.internal.warc.wet.gz',
    'CC-MAIN-20160924173739-00004-ip-10-143-35-109.ec2.internal.warc.wet.gz'
    ]
filenames = [path.join(base_dir, fn) for fn in filenames]
combined_path = ','.join(filenames)

counts = sc.textFile(combined_path)\
            .flatMap(extractWordsRegex)\
            .reduceByKey(lambda x,y: x + y)\
            .filter(lambda x: x[1] >= 30)
counts.saveAsTextFile(path.join(base_dir, 'results.txt'))

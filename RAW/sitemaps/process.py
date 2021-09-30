import argparse
import gzip
import re
import sys

import zstandard


def main(filepath: str):
    with open(filepath, 'rb') as f, open(filepath+'.items.zst', 'wb') as f2:
        data = gzip.decompress(f.read())
        items = []
        for qid in re.findall(rb'%?qid=([0-9a-zA-Z]+)', data):
            items.append(b'qid:'+qid)
        f2.write(zstandard.compress(b'\n'.join(items)))

if __name__ == '__main__':
    main(sys.argv[1])


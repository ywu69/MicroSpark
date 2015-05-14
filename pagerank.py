__author__ = 'blu2'

import zerorpc
import sys
from myRDD import *
import StringIO
import cloudpickle
from datetime import datetime
import params
from Context import Context

import re

def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    l =len(parts)
    ls = []
    for i in range(l):
        if i == 0 or parts[i].strip() == '':
            continue
        else:
            ls.append(parts[i])
    return parts[0], ls


if __name__ == '__main__':
    filename = sys.argv[1]
    master_addr = sys.argv[2]
    R = RDD(None, None, master_addr)

    links = R.TextFile(filename).map(lambda urls: parseNeighbors(urls)).cache()
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))
    ranks.collect()

    for iteration in range(1):
        # Calculates URL contributions to the rank of other URLs.
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
        # Re-calculates URL ranks based on neighbor contributions.
        ranks = contribs.reduceByKey(operator.add).mapValues(lambda rank: rank * 0.85 + 0.15)

    # Collects all URL ranks and dump them to console.
    res = ranks.collect()
    for (link, rank) in res:
        print("%s has rank: %s." % (link, rank/len(res)))
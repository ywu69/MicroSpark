__author__ = 'blu2'
import os
def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


f = lambda x: computeContribs(x[1][0], x[1][1])

for i in f(("a",[["b","c"],1.0])):
    print i


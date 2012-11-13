from pageranks import *

properties = None

configuration = {
    'inbaselocator' : '/home/giorgos/current/data/graphs/bvs/cnr-2000',
    'outbaselocator': '/home/giorgos/packages/jylab/examples/cnr-2000'
    }

rank = CompressedWebGraphRank(properties, configuration)
rank.execute()



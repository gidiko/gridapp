from pageranks import *


properties = None
configuration = {'inbaselocator' : '/home/giorgos/packages/jylab/examples/random-10000',
                 'outbaselocator' : '/home/giorgos/packages/jylab/examples/random-10000'}


rank = WebMatrixRank(properties, configuration)
rank.execute()



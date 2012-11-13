from pageranks import *

n = 10000
outdegree = 10
size = 8
generator = WebMatrixGenerator('random', n, outdegree)
generator.generate()
filepath = generator.filepath
print filepath

partitioner = WebMatrixPartitioner(filepath, size)
# print partitioner.outfilepaths
partitioner.split()
partitioner.dump()

properties = {'size' : size}
configuration = {'inbaselocator' : '/home/giorgos/packages/jylab/examples/random-10000',
                 'outbaselocator' : '/home/giorgos/packages/jylab/examples/random-10000',
                 'stopmethod' : 'NormDelta',
                 'threshold': 1e-6}
mode = 'asynchronous'

rank = ThreadedRank(properties, configuration, mode)
rank.execute()

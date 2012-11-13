########################################################################
# first start ibis-server at <ibis.server.address machine>, e.g.:
#
# giorgos@tweety:~> packages/ibis/bin/ibis-server --events
#
# and then issue:
#
# jylab <this-file>
# 
# at <size> (in number) shells!
#
########################################################################

from pageranks import *

n = 10000
outdegree = 10
size = 3
generator = WebMatrixGenerator('random', n, outdegree)
generator.generate()
filepath = generator.filepath
print filepath

partitioner = WebMatrixPartitioner(filepath, size)
# print partitioner.outfilepaths
partitioner.split()
partitioner.dump()

properties = {'ibis.implementation.path': '/home/giorgos/packages/ibis/lib',
              'ibis.server.address': '150.140.143.198',
              'ibis.pool.name': 'pagerank',
              'ibis.pool.size': str(size),
              'log4j.configuration': 'file:/home/giorgos/packages/ibis/log4j.properties'
              }

capabilities = {'ibisCapabilities' : ['ELECTIONS_STRICT',
                                      'CLOSED_WORLD',
                                      'MEMBERSHIP_TOTALLY_ORDERED',
                                      'SIGNALS'],
                'portType' : ['COMMUNICATION_RELIABLE',
                              'COMMUNICATION_FIFO',
                              'SERIALIZATION_OBJECT_SUN',
                              'RECEIVE_EXPLICIT',
                              'CONNECTION_ONE_TO_ONE',
                              'CONNECTION_DOWNCALLS']
                }


configuration = {'inbaselocator' : '/home/giorgos/packages/jylab/examples/random-10000',
                 'outbaselocator' : '/home/giorgos/packages/jylab/examples/random-10000',
                 'threshold': 1e-6}
mode = 'asynchronous'


rank = IbisParallelRank(properties, capabilities, configuration, mode)
rank.execute()

import os

script = 'jylabme.py'

job = Job()
job.application = Executable(exe=script, args=[])
job.inputsandbox = ['/home/gkollias/crawlbox/jylabme.py',
                    '/home/gkollias/crawlbox/gridme.py',
                    '/home/gkollias/crawlbox/crawlme.py',
                    '/home/gkollias/crawlbox/nutching.py',
                    '/home/gkollias/crawlbox/nutch-site.xml'
                    ]
job.backend = LCG(CE='ce01.isabella.grnet.gr:2119/jobmanager-pbs-see')
job.submit()




                                         

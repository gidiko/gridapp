#########################################################################
# Jylab                                                                 #
#                                                                       #
# GridProxy.py                                                          #
#########################################################################
#                                                                       #
# TODO : Add  myproxy-... commands for long-term proxies                #
#                                                                       #
#########################################################################

"""
Module for initialize Grid access through voms proxy and handle it

"""

# hack: the dir of the grid-proxy-.. commands

from util import *
from ssh import RemoteExecutor
from sysprocess import ProcExecutor

class Proxy:
    """ Class for the grid proxy """
    def __init__( self, executor = None ):
        """ Create the Proxy Object """
        self.__executor = executor
        if executor is None:
            self.__executor = ProcExecutor()
        x = self.__executor
        x.exec(['grid-proxy-init'])
        if isinstance(x, RemoteExecutor):
            err =  x.error()
            pwd = readPassword(err)
            if pwd == '': # HACK:Otherwise it blocks
                x.close()
                return
            x.write(pwd)
            print x.input()
        else:
            print x.error()
            print x.input()
            print x.error()

        x.close()

    def info( self ):
        """ Get proxy Info """
        x = self.__executor
        x.exec(['grid-proxy-info'])
        print x.input()
        x.close()

    def destroy( self ):
        """ Destroy Proxy """
        x = self.__executor
        x.exec(['grid-proxy-destroy'])
        x.close()

    def renew( self ):
        """ Renew proxy """
        pass

    def getIfAvailable( self ):
        """ Get proxy if available """
        pass

    def isValid( self, validity = "" ):
        """ Check if proxy is valid """
        timeleft = self.timeleft()
        if (timeleft > 0):
            print "Proxy is Valid"
            return 1
        else:
            print "proxy is not Valid"
            return 0

    def timeleft( self ):
        x = self.__executor
        x.exec(['grid-proxy-info','-timeleft'])
        input =  x.input()
        x.close()
        return int(input)

    def timeleftInHMS( self ):
        timeleft = self.timeleft()
        h = timeleft/3600
        mleft = (timeleft%3600)
        m = mleft / 60
        s = mleft%60
        print "Timeleft:"+str(h)+" hours "+str(m)+" min "+str(s)+" sec"

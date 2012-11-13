#############################################################################
#                                                                           #
#                                                                           #       
#############################################################################


# java imports
from java.lang import Runtime,System
from java.io import BufferedReader,InputStreamReader,DataOutputStream
from ch.ethz.ssh2 import StreamGobbler

from util import *

import os

class ProcExecutor:
    """ Executor class for local OS command execution through Runtime and 
    conversation functions for getting the input and error streams and writing
    to output stream"""

    def __init__(self):
        self.__instr = None
        self.__errstr = None
        self.__inputreader = None
        self.__errortreader = None
        self.__outputwriter = None
        self.__process = None
        self.__env = []

    def exec(self,command):
        """ Execute an OS command. This function also starts three threads 
        that handle the input, error and output streams. Then the other 
        functions can be used to make a conversation with the os process."""
        rt = Runtime.getRuntime()
        proc = rt.exec(command)

        self.__outputwriter = DataOutputStream(proc.getOutputStream())
            
        # start a new thread for the input stream of the process and set the
        # Reader
        self.__instr = StreamGobbler(proc.getInputStream())
        self.__inputreader = Reader(BufferedReader(InputStreamReader(self.__instr)))

        # start a new thread for error stream of the process and set the 
        # Reader
        self.__errstr = StreamGobbler(proc.getErrorStream())
        self.__errorreader = Reader(BufferedReader(InputStreamReader(self.__errstr)))
    
        self.__process = proc
        return proc

    def input(self):
        """ Function for reading the output of a process.
        Wrapper for Reader readString function. """
        if self.__inputreader is None:
            print "Error inputstream is None"
            return
        return self.__inputreader.readString()

    def error(self):
        """ Function for reading the error of a process. 
        Wrapper for Reader readString function. """
        if self.__errorreader is None:
            print "Error errorstream is None"
            return
        return self.__errorreader.readString()

    def write(self, bytes = None):
        """ Function to read from system in and write to the process 
        input (or the proc output)"""
        writer = self.__outputwriter
        if bytes is None:
            bytes = raw_input()
        for i in bytes[:]:
            writer.write(i)
            writer.flush()
        writer.write('\n')
        writer.flush()

    def getEnv(self, var):
        return os._shellEnv.environment[var]

    def setEnv(self, var, value):
        os._shellEnv.environment[var] = value

    def close(self):
        self.__instr.close()
        self.__errstr.close()
        self.__instr = None
        self.__errstr = None
        self.__process.destroy()


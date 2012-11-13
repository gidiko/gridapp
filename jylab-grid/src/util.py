#############################################################################
# Utilities:                                                                #
#   shellexec : Execute a command in the shell. Input-Ouput-Error           #
#           seems to work pretty fine.                                      #
#                                                                           #
#############################################################################

# java imports
from java.lang import Character,Thread
from java.io import BufferedReader,InputStreamReader,DataOutputStream

from java.awt import BorderLayout,Container
from java.awt.event import KeyEvent
from ch.ethz.ssh2 import StreamGobbler,Session,Connection

from jylab import PasswordField
from jline import ConsoleReader

# jython imports
import os


class Reader:
    """ Wrapper class for bufferedreader class. """
    def __init__(self, buf):
        """ Just initialise buffer """
        self.__buffer = buf

    def ready():
        """ Function to check if there is something to read. Wrapper of
        BufferedReder ready() function."""
        return self.__buffer.ready()

    def read(self):
        """ Function to read a byte. Wrapper of BufferedReder read()
        function. Blocking  """
        if self.__buffer is None:
            print "Error: buffer doesn't exist"
            return

        ch = self.__buffer.read()
        return chr(ch)

    def readString(self):
        """ Function to read a whole string. Blocking only at start.This means
        that it blocks at the beginning, reads at least one byte and if thereis
        no more to read it just returns. """
        if self.__buffer is None:
            print "Error: buffer doesn't exist"
            return
        buf = self.__buffer
        # block so as to wait to produce output from the processs or session
        ch = buf.read()
        if ch == -1:
            return ""
        result = []
        result.append(chr(ch))
        # check if buffer is ready so as to read, if and only if it has
        # somethging to read
        while buf.ready():
            ch = buf.read()
            if (ch == -1):
                break
            result.append(chr(ch))
        return "".join(result)


class Writer:
    """ Wrapper Class for DataOutputStream writer """
    def __init__(self, _writer):
        self.__writer = _writer

    def writeOutput(self, bytes):
        """ Write a string to output """
        for i in bytes[:]:
            self.__writer.write(i)

        writer.write('\n')
        writer.flush()

    def readWrite(self):
        """ Read a string and write it to output (Redirect System.in) """
        bytes = raw_input()
        for i in bytes[:]:
            self.__writer.write(i)

        writer.write('\n')
        writer.flush()



class userdir:
	def __init__(self):
		self.userdir = os.path.gethome()


def getuserdir():
    return userdir().userdir

def creategridtemp(ex):
    ex.exec('mkdir -p ~/.jylab/grid/temp')
    ex.input()
    ex.close()
    ex.exec('mkdir -p /tmp/jobOutput')
    ex.input()
    ex.close()

def getuitemp(userhome):
    return userhome+'/.jylab/grid/temp/'

#def griduserhome(ex):
#    ex.env

# Useful Functions
def readPassword(title):
    #return raw_input("Password:")
    #return ConsoleReader().readLine(Character('*'))
    #print "getting password"
    passField = PasswordField(title)
    psw = passField.getPassword()
    return psw

def raw_input(text):
    from javax.swing import JOptionPane
    inp = JOptionPane.showInputDialog(text)
    return inp

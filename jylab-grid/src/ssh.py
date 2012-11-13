#
# A collection of classes for facilitating remote execution of commands,
# transfer of files... over ssh connections
#

from java.io import BufferedReader,InputStreamReader,DataOutputStream,File
from ch.ethz.ssh2 import StreamGobbler,Session,Connection,SCPClient
from util import readPassword,Reader,raw_input
from java.lang import IllegalStateException,String
import os,re


class RemoteHost:
    """Examples of use:
    remotehost = RemoteHost('gkollias@isabella', '/home/giorgos/.ssh/id_rsa')
    #establishes ssh connection
    """
    def __init__(self, address = None, keypath=" "):
        """An established ssh connection to address.

        address is of the form user@host:port where the :port part is optional.
        keypath is the path to the local private key.
        """

        user, host, port = self.parse(address)
        self.connection = Connection(host, port)
        self.connection.connect()
        print "Loging in to ssh server " + host
        try:
            if os.path.isfile(keypath):
                keyfile = File(keypath)
                self.connection.authenticateWithPublicKey(user, keyfile, None)
            else:
                psw = readPassword("UI Password:")
                self.connection.authenticateWithPassword(user,psw)
        except Exception:
            print "Authentication Failed"
            return None

        self.user = user
        self.host = host
        self.keypath = keypath
        self.port = port
        #self.connection = connection
        #TODO:Make env a dictionary
        self.env = getSSHEnv(self.connection)
        self.keepAlive = 1
        self.startKeepAlive()

    def parse(self, address):
        """address (sth of the form user@host:port) is converted to
        [user, host, port].
        """
        if address is None:
            hostport = raw_input("UI Host:")
            user = raw_input("UI Username:")
        else:
            user, hostport = address.split('@')

        if ':' in hostport:
            host, port = hostport.split(':')
        else:
            host, port = hostport, 22
        port = int(port)
        return [user, host, port]


    def startKeepAlive(self, interval = 60000):
        import java.lang as lang

        self.keepAlive = 1
        command = "/bin/date"
        rhost = self

        class NestedRunnable(lang.Runnable):
            def run(self):
                while (rhost.keepAlive == 1):
                    lang.Thread.sleep(interval)
                    sess = rhost.connection.openSession()
                    sess.execCommand(command) # execute command
                    inputreader = BufferedReader(InputStreamReader(sess.getStdout()))
                    inputreader.readLine()
                    sess.close()


        self.keepAliveThread = lang.Thread(NestedRunnable())
        self.keepAliveThread.start()

    def stopKeepAlive(self):
        self.keepAlive = 0
        try:
            self.keepAliveThread.interrupt()
        except:
            pass

    def destroy(self):
        """ Close Connection Sessions and Streams """
        self.connection.close()
        self.stopKeepAlive()

class RemoteExecutor:
    """ Execute a command to the remote host through ssh session. This function
    also starts three threads that handle the input, error and output streams.
    Then the other functions can be used for conversating with the process.

    remexecutor.exec('ls -al') # prints remote home directory contents
    """

    def __init__(self, remotehost):
        """ Initialize the connection."""
        self.__connection = remotehost.connection
        self.__env = remotehost.env
        self.__session = self.__connection.openSession()
        self.__instr = None
        self.__errstr = None
        self.__inputreader = None
        self.__errortreader = None
        self.__outputwriter = None

    def exec(self,command):
        if not self.__connection.isAuthenticationComplete():
            print "Connection not established"
            return

        if self.__session == None:
          self.__session = self.__connection.openSession()
        sess = self.__session

        if type(command) is type([]): # if command is a list make it a string
            command = " ".join(command)

        # make environment variables to string and assemble command
        environment = " ".join(["=".join(i) for i in self.__env])
        command = "export " + environment + " && " + command

        sess.execCommand(command) # execute command
        self.__outputwriter = DataOutputStream(sess.getStdin())

        # start a new thread for the input stream of the process and set the
        # Reader
        self.__instr = StreamGobbler(sess.getStdout())
        self.__inputreader = Reader(BufferedReader(InputStreamReader(self.__instr)))

        # start a new thread for error stream of the process and set the
        # Reader
        self.__errstr = StreamGobbler(sess.getStderr())
        self.__errorreader = Reader(BufferedReader(InputStreamReader(self.__errstr)))

    def input(self):
        """ Function for reading the output of a process.
        Wrapper for Reader readString function.
        """
        if self.__inputreader is None:
            print "Error __inputstreamer__ is None"
            return
        return self.__inputreader.readString()

    def error(self):
        """ Function for reading the error of a process.
        Wrapper for Reader readString function.
        """
        if self.__errorreader is None:
            print "Error __errorstreamer__ is None"
            return
        return self.__errorreader.readString()

    def write(self, bytes = None):
        """ Function to read from system in and write to the process
        input (or the proc output)
        """
        writer = self.__outputwriter
        if bytes is None:
            bytes = raw_input()
        #for i in bytes[:]:
        #  print ord(i)
        writer.writeBytes(bytes+"\n")
        writer.flush()

    def getEnv(self, var):
        env = self.__env
        for i in env:
            if var in i:
                return i[1]

    def setEnv(self, var, value):
        env = self.__env
        curvar = None
        for i in range(len(env)):
            if var in env[i]:
                curvar = env[i][1]
                del env[i]
                break

        self.__env.append((var,value))

    def close(self):
        self.__instr.close()
        self.__errstr.close()
        self.__session.close()
        self.__instr = None
        self.__errstr = None
        self.__session = None


class FileTransfer:
    def __init__(self, remotehost):
        """Facilitates the exchange of files over an established
        ssh connection (bridge)

        filetransfer.download(['~/geodise-1.log'], '.') #gets a file from remote machine
        filetransfer.upload(['./t01.py'], '~') #puts a file to remote home directory
        """
        connection = remotehost.connection
        scpclient = SCPClient(connection)
        self.remotehost = remotehost
        self.connection = connection
        self.scpclient = scpclient

    def upload(self, localfiles, remotedir):
        """"Uploads" a list of files to a remote directory.
        """
        self.scpclient.put(localfiles, remotedir)

    def download(self, remotefiles, localdir):
        """"Downloads" a list of remote files to a local directory.
        """
        self.scpclient.get(remotefiles, localdir)



# Function to get the ssh environment variables. If executing remote command
# with session.execCommand then sssh-daemon executes this command, so
# we must set the correct environment

def getSSHEnv(connection):
    """ Get the environment variables of an ssh remote server """
    print "Getting Environment Variables ..."
    '''
    sess = connection.openSession()
    sess.requestDumbPTY()
    sess.startShell()
    instr = StreamGobbler(sess.getStdout())
    stdin = BufferedReader(InputStreamReader(instr))
    # get the login shell information.
    #stdin.readLine()    # just delay it so as to be synchronized
    #stdin.readLine()    # just delay it so as to be synchronized
    while(1):
      c = stdin.read()
      if chr(c) == "]":
        c = stdin.read()
        if chr(c) == "$":
          break
    stdin.read()
    out = DataOutputStream(sess.getStdin())
    out.writeBytes("printenv\n")
    input = []
    flag = 0
    line = ""
    while 1:
      c = stdin.read()
      if chr(c) == "\n":
        input.append(line)
        line = ""
      else:
        line = line + chr(c);

      if chr(c) == "]":
        c = stdin.read()
        if chr(c) == "$":
          break
    environ = "".join(input)
    env = re.findall('(\S+)=(\S+)',environ)
    instr.close()
    out.close();
    out = None
    instr = None
    stdin = None
    sess.close()
    sess = None
    return env
    '''

    sess = connection.openSession()
    sess.requestDumbPTY()
    sess.startShell()
    instr = StreamGobbler(sess.getStdout())
    stdin = BufferedReader(InputStreamReader(instr))

    #wait
    while 1==1:
      c = stdin.read() # read the rest bytes before issueing cmd
      if chr(c) == "]":
        c = stdin.read()
        if chr(c) == "$":
          break

    out = DataOutputStream(sess.getStdin())
    #issue the command plus echo something(FINISH) to know when to unblock
    out.writeBytes("printenv && echo FINISH\n")
    input = []
    flag = 0
    while 1:
        line = stdin.readLine()
        if line is None:
            break
        line = line + "\n"
        input.append(line)
        if line.endswith("FINISH\n"):
            if flag == 1:
                break
            else:
                flag +=1
    environ = "".join(input)
    env = re.findall('(\S+)=(\S+)\n',environ)
    instr.close()
    instr = None
    stdin = None
    sess.close()
    sess = None
    return env


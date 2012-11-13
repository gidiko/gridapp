from ibis.ipl import *
from java.lang import *
from no.uib.cipr.matrix import Matrices
from java.util.concurrent.locks import ReentrantLock
from webmatrix import *
import jarray
import os
import threading
import webmatrix


########################################################################
# Node
########################################################################

class Node:
    '''
    A communication engine; a basis for building a communicator.
    '''
    pass


########################################################################
# Port
########################################################################

class Port:
    '''
    Communication end point for connecting two or more communicators
    each in its own process.
    '''
    pass

class Inport(Port):
    '''
    A port waiting for incoming connections.

    After the connection is established it serves as the gate for incoming
    messages.
    '''
    pass

class Outport(Port):
    '''
    A port initiating a connection to another one.

    After the connection is established the port serves as the gate for
    forwarding messages to other processes.
    '''
    pass



########################################################################
# Communicator
########################################################################

class Communicator:
    '''
    A communication engine built to serve computing threads.

    In its simplest form it contains boxes permitting concurrent access by
    multiple threads both communication and computation ones.
    '''
    def setupProperties(self, properties):
        '''
        @param properties:
        '''
        pass


    def initBoxes(self, datalist):
        '''
        Initialize the boxes of the communicator.

        @param datalist: list of objects to populate the boxes of
        the communicator
        '''

        pass


    def init(self, datalist):
        '''
        Initialize the communicator.

        @param datalist: list of objects to populate the boxes of
        the communicator
        '''
        pass




class ThreadCommunicator(Communicator):
    '''
    A shared communicator where all threads execute within the context
    of a single process.
    '''
    def resetStageFlags(self):
        self.initInboxesOK = False
        self.initRankersOK = False
        self.startRankersOK = False




class ProcessCommunicator(Communicator):
    '''
    A communicator used for the communication of computing threads executing
    within different processes.
    '''
    def resetStageFlags(self):
        # set some boolean flags to false
        self.setupNodeOK = False
        self.setupInportsOK = False
        self.setupConnectedOutportsOK = False
        self.initInboxesOK = False
        self.initOutboxesOK = False
        self.initSendersOK = False
        self.initReceiversOK = False
        self.startSendersOK = False
        self.startReceiversOK = False

    def setupNode(self):
        '''
        Configure the communication engine used by the communicator.
        '''
        pass

    def setupInports(self):
        '''
        Configure ports used for incoming messages.
        '''
        pass

    def setupConnectedOutports(self):
        '''
        Configure ports used for outgoing messages and connect them to their
        peer ports waiting for connections.
        '''
        pass

    def setupConnectedPorts(self):
        '''
        Connect all ports as necessary, e.g. build the communication clique.
        '''
        pass

    def initInboxes(self, datalist):
        '''
        Initialize boxes used for holding incoming messages.

        @param datalist: list of initial placeholder objects
        '''
        pass

    def initOutboxes(self, datalist):
        '''
        Initialize boxes used for holding outgoing messages.

        @param datalist: list of initial objects
        '''
        pass

    def initSenders(self):
        '''
        Initialize sending threads; don't start them yet.
        '''
        pass

    def initReceivers(self):
        '''
        Initialize receiving threads; don't start them yet.
        '''
        pass

    def init(self, datalist):
        '''
        Initialize the communicator by initializing boxes and threads.

        @param datalist: list of objects to be initially put into boxes.
        '''
        pass

    def startSenders(self):
        '''
        Start sending threads.
        '''
        pass

    def startReceivers(self):
        '''
        Start receiving threads.
        '''
        pass

    def start(self):
        '''
        Start the communicator.

        This follows the initialization phase; starts the communication threads
        '''
        pass


class IbisCommunicator(ProcessCommunicator):
    '''
    A communicator using an Ibis instance as its communication engine.
    '''
    def __init__(self, properties, capabilities):
        '''
        Constructor.

        @param properties: environment properties
        @param capabilities: ibis and port capabilities
        '''
        self.resetStageFlags()
        self.setupProperties(properties)
        self.setupCapabilities(capabilities)
        self.setupNode()
        self.setupConnectedPorts()


    def setupProperties(self, properties):
        '''

        @param properties:
        '''
        # populate system properties
        keys = properties.keys()
        for key in keys:
            value = properties[key]
            System.setProperty(key, value)
        print 'populated system properties'
        self.properties = properties


    def setupCapabilities(self, capabilities):
        '''

        @param capabilities:
        '''
        # setup portType
        pt = []
        ptclass = Class.forName('ibis.ipl.PortType')
        for cap in capabilities['portType']:
            field = ptclass.getField(cap)
            value = field.get(None)
            pt.append(value)
        portType = PortType(pt)
        print 'setup port type'

        # setup ibisCapabilities
        icaps = []
        icclass = Class.forName('ibis.ipl.IbisCapabilities')
        for cap in capabilities['ibisCapabilities']:
            field = icclass.getField(cap)
            value = field.get(None)
            icaps.append(value)
        ibisCapabilities = IbisCapabilities(icaps)
        print 'setup ibis capabilities'

        self.portType = portType
        self.ibisCapabilities = ibisCapabilities
        self.capabilities = capabilities


    def setupNode(self):
        self._setupIbis()


    def _setupIbis(self):
        # instantiate ibis
        if self.setupNodeOK:
            return
        ibis = IbisFactory.createIbis(self.ibisCapabilities, None, [self.portType])
        print 'instantiated ibis'

        registry = ibis.registry()
        from ibis.poolInfo import PoolInfo
        info = PoolInfo(System.getProperties(), True)
        rankID = info.rank()
        size = info.size()

        # peers (identifiers of all ibis instances within the communication clique)
        # access key = #localrank
        peers = {}
        myID = registry.elect('#%d' % rankID)
        for i in range(size):
            name = '#%d' % i
            peer = registry.getElectionResult(name)
            peers[name] = peer
        self.ibis = ibis
        self.rankID = rankID
        self.size = size
        self.peers = peers

        self.registry = registry
        self.myID = myID
        self.setupNodeOK = True


    def setupInports(self):
        # inports (identifiers of ports listening for connections and receiving data)
        # access key = #localrank <- #remoterank
        if self.setupInportsOK:
            return
        inports = {}
        for i in range(self.size):
            if i != self.rankID:
                name = '#%d <- #%d' %  (self.rankID, i)
                port =  self.ibis.createReceivePort(self.portType, name)
                port.enableConnections()
                inports[name] = port
        self.inports = inports
        self.setupInportsOK = True


    def setupConnectedOutports(self):
        # outports (identifiers of ports for sending data)
        # access key = #localrank -> #remoterank
        if self.setupConnectedOutportsOK:
            return
        outports = {}
        for i in range(self.size):
            if i != self.rankID:
                peername = '#%d' % i
                peer = self.peers[peername]
                localportname = '#%d -> #%d' % (self.rankID, i)
                remoteportname = '#%d <- #%d' % (i, self.rankID)
                port = self.ibis.createSendPort(self.portType)
                #connection retries
                #port.connect(peer, remoteportname, 1000, True)
                port.connect(peer, remoteportname)
                outports[localportname] = port
        self.outports = outports
        self.setupConnectedOutportsOK = True


    def setupConnectedPorts(self):
        self.setupInports()
        self.setupConnectedOutports()



    def __str__(self):
        '''
        Communicator information.
        '''
        import cStringIO
        output = cStringIO.StringIO()
        if self.setupNodeOK:
            print >> output, 'myID: %s' % self.myID
            print 'peerIDs: %s' % self.peers
        if self.setupInportsOK:
            print >> output, 'inports: %s' % self.inports
        if self.setupConnectedOutportsOK:
            print >> output, 'connected outports: %s' % self.outports
        if self.initInboxesOK:
            print >> output, 'inboxes: %s' % self.inboxes
        if self.initOutboxesOK:
            print >> output, 'outboxes: %s' % self.outboxes
        if self.initSendersOK:
            print >> output, 'senders: %s' % self.senders
        if self.initReceiversOK:
            print >> output, 'receivers: %s' % self.receivers
        s = output.getvalue()
        output.close()
        return s




class ThreadRankletCommunicator(ThreadCommunicator):
    '''
    A communicator hosting threads using shared boxes.
    '''

    def __init__(self, properties, mode = 'asynchronous'):
        '''

        @param properties:
        @param mode: synchronous or asynchronous
        '''
        self.mode = mode
        self.resetStageFlags()
        self.setupProperties(properties)


    def setupProperties(self, properties):
        size = properties['size']
        self.size = size


    def initAsynchronousModeInboxes(self, ranklets):
        '''
        Initialize boxes for use with threads asynchronously accessing them

        @param ranklets: list of rank parts
        '''
        if self.initInboxesOK:
            return
        inboxes = {}
        for i in range(self.size):
            name = '#%d' %  i
            box = AsynchronousRankletInbox(ranklets[i], name)
            inboxes[name] = box
            box.verbose = False
        box.verbose = False
        self.inboxes = inboxes
        self.initInboxesOK = True


    def initSynchronousModeInboxes(self, ranklets):
        '''
        Initialize boxes for use with threads synchronously accessing them

        @param ranklets: list of rank parts
        '''
        if self.initInboxesOK:
            return
        inboxes = {}
        for i in range(self.size):
            name = '#%d' %  i
            box = SynchronousRankletInbox(ranklets[i], name)
            inboxes[name] = box
            box.verbose = False
        box.verbose = False
        self.inboxes = inboxes
        self.initInboxesOK = True


    def initInboxes(self, ranklets):
        if self.mode == 'asynchronous':
            self.initAsynchronousModeInboxes(ranklets)
        else:
            self.initSynchronousModeInboxes(ranklets)


    def initRankers(self, parameterslist):
        '''
        Initialize the rankers accessing the shared boxes in the communicator.

        @param parameterslist: list of parameters for each ranker
        '''
        # rankers (threads countinously computing)
        # access key = #rank
        if self.initRankersOK:
            return
        rankers = {}
        for i in range(self.size):
            rankerID = i
            name = '#%d' % rankerID
            parameters = parameterslist[i]
            ranker = ThreadedRanker(self, parameters)
            rankers[name] = ranker
            ranker.verbose = False
        self.rankers = rankers
        self.initRankersOK = True


    def startRankers(self):
        '''
        Start rankers.
        '''
        if self.startRankersOK:
            return
        for i in range(self.size):
            rankerID = i
            name = '#%d' % rankerID
            self.rankers[name].start()
            #XXX print 'starting ranker %d' % rankerID #XXX
        self.startRankersOK = True


    def initBoxes(self, ranklets):
        # boxes (thread-safe ranklet, accessed by sender, receiver and computing threads)
        # access key = #localrank
        self.initInboxes(ranklets)



    def start(self):
        self.startRankers()



class IbisRankletCommunicator(IbisCommunicator):
    '''
    A communicator for exchanging messages between processes executing in parallel.

    '''
    def __init__(self, properties, capabilities, mode='asynchronous'):
        self.mode = mode
        IbisCommunicator.__init__(self, properties, capabilities)


    def initAsynchronousModeInboxes(self, ranklets):
        if self.initInboxesOK:
            return
        inboxes = {}
        for i in range(self.size):
            if i != self.rankID:
                name = '#%d <- #%d' %  (self.rankID, i)
                box = AsynchronousRankletInbox(ranklets[i], name)
                inboxes[name] = box
                box.verbose = False
        name = '#%d <- #%d' % (self.rankID, self.rankID)
        box = SequentialRankletBox(ranklets[self.rankID], name)
        inboxes[name] = box
        box.verbose = False
        self.inboxes = inboxes
        self.initInboxesOK = True


    def initAsynchronousModeOutboxes(self, ranklets):
        if self.initOutboxesOK:
            return
        outboxes = {}
        for i in range(self.size):
            if i != self.rankID:
                name = '#%d -> #%d' %  (self.rankID, i)
                box = AsynchronousRankletOutbox(ranklets[i], name)
                outboxes[name] = box
                box.verbose = False
        self.outboxes = outboxes
        self.initOutboxesOK = True


    def initSynchronousModeInboxes(self, ranklets):
        if self.initInboxesOK:
            return
        inboxes = {}
        for i in range(self.size):
            if i != self.rankID:
                name = '#%d <- #%d' %  (self.rankID, i)
                box = SynchronousRankletInbox(ranklets[i], name)
                inboxes[name] = box
                box.verbose = False
        name = '#%d <- #%d' % (self.rankID, self.rankID)
        box = SequentialRankletBox(ranklets[self.rankID], name)
        inboxes[name] = box
        box.verbose = False
        self.inboxes = inboxes
        self.initInboxesOK = True


    def initSynchronousModeOutboxes(self, ranklets):
        if self.initOutboxesOK:
            return
        outboxes = {}
        for i in range(self.size):
            if i != self.rankID:
                name = '#%d -> #%d' %  (self.rankID, i)
                box = SynchronousRankletOutbox(ranklets[i], name)
                outboxes[name] = box
                box.verbose = False
        self.outboxes = outboxes
        self.initOutboxesOK = True


    def initInboxes(self, ranklets):
        if self.mode == 'asynchronous':
            self.initAsynchronousModeInboxes(ranklets)
        else:
            self.initSynchronousModeInboxes(ranklets)


    def initOutboxes(self, ranklets):
        if self.mode == 'asynchronous':
            self.initAsynchronousModeOutboxes(ranklets)
        else:
            self.initSynchronousModeOutboxes(ranklets)

    def initBoxes(self, ranklets):
        self.initInboxes(ranklets)
        self.initOutboxes(ranklets)


    def initSenders(self):
        # senders (threads countinously inspecting their box and sending away its content through their port)
        # access key = #localrank -> #remoterank
        if self.initSendersOK:
            return
        senders = {}
        for i in range(self.size):
            if i != self.rankID:
                portname = '#%d -> #%d' %  (self.rankID, i)
                boxname = portname
                port = self.outports[portname]
                box = self.outboxes[boxname]
                name = portname
                sender = IbisRankletSender(port, box)
                senders[name] = sender
                sender.enable()
                sender.verbose = False
                self.senders = senders
        self.initSendersOK = True


    def initReceivers(self):
        # receivers (threads countinously inspecting their port for incoming messages and saving them in their box)
        # access key = #localrank <- #remoterank
        if self.initReceiversOK:
            return
        receivers = {}
        for i in range(self.size):
            if i != self.rankID:
                portname = '#%d <- #%d' %  (self.rankID, i)
                boxname = portname
                port = self.inports[portname]
                box = self.inboxes[boxname]
                name = portname
                receiver = IbisRankletReceiver(port, box)
                receivers[name] = receiver
                receiver.enable()
                receiver.verbose = False
        self.receivers = receivers
        self.initReceiversOK = True

    def init(self, ranklets):
        self.initBoxes(ranklets)
        self.initSenders()
        self.initReceivers()


    def startSenders(self):
        if self.startSendersOK:
            return
        for i in range(self.size):
            if i != self.rankID:
                name = '#%d -> #%d' %  (self.rankID, i)
                self.senders[name].start()
        self.startSendersOK = True


    def startReceivers(self):
        if self.startReceiversOK:
            return
        for i in range(self.size):
            if i != self.rankID:
                name = '#%d <- #%d' %  (self.rankID, i)
                self.receivers[name].start()
        self.startReceiversOK = True


    def start(self):
        self.startSenders()
        self.startReceivers()


########################################################################
# Sender
########################################################################

class Sender:
    '''
    A sending thread.
    '''
    def send(self):
        '''
        Send attached data.
        '''
        pass

    def enable(self):
        pass

    def disable(self):
        pass

    def run(self):
        pass


class IbisRankletSender(Thread, Sender):
    '''
    A sending thread using Ibis communication engine also forwarding rank parts.
    '''
    def __init__(self, port, box):
        '''
        Initialize sender.

        @param port: attached port
        @param box: attached box
        '''
        Thread.__init__(self)
        self.port  = port
        self.box = box
        self.enabled = False
        self.verbose = False


    def send(self):
        ranklet = self.box.get()
        message = self.port.newMessage()
        message.writeObject(ranklet)
        message.finish()
        if self.verbose:
            print self.infostring(ranklet)


    def infostring(self, ):
        start = ranklet.getStart()
        end = ranklet.getEnd()
        return 'sender[%s]: [%d, %d)' % (self.box.name, start, end)


    def enable(self):
        self.enabled = True


    def disable(self):
        self.enabled = False


    def run(self):
        counter = 100
        while(self.enabled and counter > 0):
            self.send()
            counter = counter - 1
            #print 'send counter = %d' % counter



########################################################################
# Receiver
########################################################################

class Receiver:
    '''
    A receiving thread.
    '''
    def receive(self):
        '''
        Receive data in the attached area.
        '''
        pass

    def enable(self):
        pass

    def disable(self):
        pass

    def run(self):
        pass




class IbisRankletReceiver(Thread, Receiver):
    '''
    A receiving thread using Ibis communication engine also importing rank parts.
    '''
    def __init__(self, port, box):
        Thread.__init__(self)
        self.port  = port
        self.box = box
        self.enabled = False
        self.verbose = False


    def receive(self):
        message = self.port.receive()
        ranklet = message.readObject()
        message.finish()
        self.box.set(ranklet)
        if self.verbose:
            print self.infostring(ranklet)


    def infostring(self, ranklet):
        start = ranklet.getStart()
        end = ranklet.getEnd()
        return 'receiver[%s]: [%d, %d)' % (self.box.name, start, end)


    def enable(self):
        self.enabled = True


    def disable(self):
        self.enabled = False


    def run(self):
        counter = 100
        while(self.enabled and counter > 0):
            self.receive()
            counter = counter - 1
            #print 'receive counter = %d' % counter



########################################################################
# Box
########################################################################

class Box:
    '''
    A data container.
    '''
    def get(self):
        pass

    def set(self):
        pass



class SequentialBox(Box):
    pass


class ConcurrentBox(Box):
    pass


class SynchronousInbox(ConcurrentBox):
    pass


class SynchronousOutbox(ConcurrentBox):
    pass


class AsynchronousInbox(ConcurrentBox):
    pass


class AsynchronousOutbox(ConcurrentBox):
    pass




class SequentialRankletBox(SequentialBox):
    '''
    A ranklet container used in sequential implementations
    '''
    def __init__(self, ranklet, name=''):
        self.ranklet = ranklet
        self.name = name
        self.verbose = False


    def get(self):
        ranklet = self.ranklet.copy()
        if self.verbose:
            print 'get%s' % self.infostring(ranklet)
        return ranklet


    def set(self, ranklet):
        self.ranklet = ranklet.copy()
        if self.verbose:
            print 'set%s' % self.infostring(ranklet)


    def infostring(self, ranklet):
        start = ranklet.getStart()
        end = ranklet.getEnd()
        return '[%s]: [%d, %d)' % (self.name, start, end)



class ConcurrentRankletBox(ConcurrentBox):
    '''
    A ranklet container used in concurrent implementations
    '''
    def __init__(self, ranklet, name=''):
        self.lock = ReentrantLock()
        self.ranklet = ranklet
        self.name = name
        self.verbose = False


    def get(self):
        try:
            self.lock.lock()
            ranklet = self.ranklet.copy()
        finally:
            self.lock.unlock()
        if self.verbose:
            print 'get%s' % self.infostring(ranklet)
        return ranklet


    def set(self, ranklet):
        try:
            self.lock.lock()
            self.ranklet = ranklet.copy()
        finally:
            self.lock.unlock()
        if self.verbose:
            print 'set%s' % self.infostring(ranklet)


    def infostring(self, ranklet):
        start = ranklet.getStart()
        end = ranklet.getEnd()
        return '[%s]: [%d, %d)' % (self.name, start, end)




class SynchronousRankletInbox(ConcurrentRankletBox, SynchronousInbox):
    '''
    A ranklet container used in concurrent implementations for hosting
    received data, synchronous mode case.
    '''
    def __init__(self, ranklet, name=''):
        ConcurrentRankletBox.__init__(self, ranklet, name)
        self.notNew = self.lock.newCondition()
        self.hasNew = True

    def get(self):
        try:
            self.lock.lock()
            while (not self.hasNew):
                self.notNew.await()
            ranklet = self.ranklet.copy()
            self.hasNew = False
        finally:
            self.lock.unlock()
        if self.verbose:
            print 'get%s' % self.infostring(ranklet)
        return ranklet


    def set(self, ranklet):
        try:
            self.lock.lock()
            self.ranklet = ranklet.copy()
            self.hasNew = True
            self.notNew.signal()
        finally:
            self.lock.unlock()
        if self.verbose:
            print 'set%s' % self.infostring(ranklet)



class SynchronousRankletOutbox(ConcurrentRankletBox, SynchronousOutbox):
    '''
    A ranklet container used in concurrent implementations for hosting
    to be sent data, synchronous mode case.
    '''
    def __init__(self, ranklet, name=''):
        ConcurrentRankletBox.__init__(self, ranklet, name)
        self.notNew = self.lock.newCondition()
        self.hasNew = False

    def get(self):
        try:
            self.lock.lock()
            while (not self.hasNew):
                self.notNew.await()
            ranklet = self.ranklet.copy()
            self.hasNew = False
        finally:
            self.lock.unlock()
        if self.verbose:
            print 'get%s' % self.infostring(ranklet)
        return ranklet


    def set(self, ranklet):
        try:
            self.lock.lock()
            self.ranklet = ranklet.copy()
            self.hasNew = True
            self.notNew.signal()
        finally:
            self.lock.unlock()
        if self.verbose:
            print 'set%s' % self.infostring(ranklet)



class AsynchronousRankletInbox(ConcurrentRankletBox, AsynchronousInbox):
    '''
    A ranklet container used in concurrent implementations for hosting
    received data, asynchronous mode case.
    '''
    def __init__(self, ranklet, name=''):
        ConcurrentRankletBox.__init__(self, ranklet, name)




class AsynchronousRankletOutbox(ConcurrentRankletBox, AsynchronousOutbox):
    '''
    A ranklet container used in concurrent implementations for hosting
    to be sent data, asynchronous mode case.
    '''
    def __init__(self, ranklet, name=''):
        ConcurrentRankletBox.__init__(self, ranklet, name)
        self.notNew = self.lock.newCondition()
        self.hasNew = False

    def get(self):
        try:
            self.lock.lock()
            while (not self.hasNew):
                self.notNew.await()
            ranklet = self.ranklet.copy()
        finally:
            self.lock.unlock()
        if self.verbose:
            print 'get%s' % self.infostring(ranklet)
        return ranklet


    def set(self, ranklet):
        try:
            self.lock.lock()
            self.ranklet = ranklet.copy()
            self.hasNew = True
            self.notNew.signal()
        finally:
            self.lock.unlock()
        if self.verbose:
            print 'set%s' % self.infostring(ranklet)




########################################################################
# IterativeAlgorithm
########################################################################

class IterativeAlgorithm:
    '''
    An iterative algorithm
    '''
    def step(self):
        '''
        Single step the iterative algorithm.
        '''
        pass

    def converged(self):
        '''
        Has the iterative algorithm converged?
        '''
        pass

    def updateError(self):
        '''
        Update the internally saved value of the error.
        '''
        pass



class SequentialIterativeAlgorithm(IterativeAlgorithm):
    '''
    A sequential iterative algorithm.

    Only a single flow of computation is involved
    '''
    pass


class ConcurrentIterativeAlgorithm(IterativeAlgorithm):
    '''
    A concurrent iterative algorithm.

    Only one computing flow is represented by an object of this type.
    In all, multiple computing flows are involved.
    These computing flows can be  either threads in a single process
    accessing a shared state or threads each within its own process
    communicating with each other by means of an
    interprocess communication mechanism, notably a socket-based one.
    '''
    def run(self):
        '''
        The code executed by the computing flow.
        '''
        pass

    def distribute(self):
        '''
        "Send" data to all other computing flows.
        '''
        pass

    def assemble(self):
        '''
        "Receive" data from all other computing flows.
        '''
        pass



class ThreadedIterativeAlgorithm(ConcurrentIterativeAlgorithm):
    '''
    A concurrent iterative algorithm computing flow implemented
    as a thread within a process.

    Peer computing flows are also implemented as threads within the same
    process.
    '''
    pass


class ParallelIterativeAlgorithm(ConcurrentIterativeAlgorithm):
    '''
    A concurrent iterative algorithm computing flow implemented
    as a thread within a process.

    Peer computing flows are also implemented as threads each executing
    within its own process. Processes communicate with each other by means of
    an interprocess communicaton mechanism.
    '''
    pass


class SequentialRanker(SequentialIterativeAlgorithm):
    '''
    A sequential algorithm (single computing flow) for ranking.
    '''
    pass

class ConcurrentRanker(ConcurrentIterativeAlgorithm):
    '''
    A concurrent algorithm (multiple computing flows) for ranking.
    '''
    pass


class WebMatrixRanker(SequentialRanker):
    '''
    A sequential algorithm (single computing flow) for ranking;
    webmatrix formatted representation of link structure is used.
    '''
    def __init__(self, parameters):
        self.setupParameters(parameters)
        self.loadGraph()
        self.loadPreferencesVector()
        self.loadInitialPageRank()

        self.iterations = 0
        self.errors = []
        self.error = self.threshold + 1.0



    def setupParameters(self, parameters):
        '''
        Import computing parameters as attributes of the object.

        @param parameters:computing parameters
        '''
        graphpath = parameters['graphpath']
        initialpagerankvectorpath = parameters.get('initialpagerankvectorpath', None)
        preferencesvectorpath = parameters.get('preferencesvectorpath', None)
        pagerankvectorpath = parameters['pagerankvectorpath']
        alpha = parameters.get('alpha', 0.85)
        threshold = parameters.get('threshold', 1e-4)
        maxiterations = parameters.get('maxiterations', 30)
        stopmethod = parameters.get('stopmethod', 'IterationNumber') # the other being 'NormDelta'

        self.graphpath = graphpath
        self.initialpagerankvectorpath = initialpagerankvectorpath
        self.preferencesvectorpath = preferencesvectorpath
        self.pagerankvectorpath = pagerankvectorpath
        self.alpha = alpha
        self.threshold = threshold
        self.maxiterations = maxiterations
        self.stopmethod = stopmethod





    def loadGraph(self):
        '''
        Loads graph as stored in a file.
        '''
        graph = Graphlet.load(self.graphpath)
        n = graph.getN()
        self.graph = graph
        self.n = n


    def loadPreferencesVector(self):
        '''
        Loads preferences vector as stored in a file.
        '''
        from webmatrix.util import DoubleArrays
        if self.preferencesvectorpath is not None:
            datamatrix =  DoubleArrays.load(self.preferencesvectorpath)
            preferencesvector = DoubleArrays.flatten(datamatrix)
        else:
            value = 1.0 / self.n
            preferencesvector = DoubleArrays.repeat(value, self.n)
        self.preferencesvector = preferencesvector




    def loadInitialPageRank(self):
        '''
        Loads initial PageRank vector as stored in a file.
        '''
        if self.initialpagerankvectorpath is not None:
            from webmatrix.util import DoubleArrays
            from webmatrix import PageRank
            datamatrix = DoubleArrays.load(self.initialpagerankvectorpath)
            initialpagerankvector = DoubleArrays.flatten(datamatrix)
            initialpagerank = PageRank(initialpagerankvector)
        else:
            from webmatrix import Ranks
            initialpagerank = Ranks.newUniformRank(self.n)
        self.initialpagerank = initialpagerank
        self.pagerank = initialpagerank


    def compute(self):
        '''
        Compute (i.e. step again and again)
        until convergence condition is satisfied.
        '''
        while not self.converged():
            self.step()
            self.updateError()
            self.iterations = self.iterations + 1



    def step(self):
        self.previouspagerank = self.pagerank
        self.pagerank = self.graph.mult(self.previouspagerank, self.alpha, self.preferencesvector)



    def converged(self):
        if (self.stopmethod == 'IterationNumber'):
            if (self.iterations > self.maxiterations):
                return True
            return False
        elif (self.stopmethod == 'NormDelta'):
            if (self.error < self.threshold and self.iterations > 0):
                return True
            return False


    def updateError(self):
        self.error = Ranks.diffNorm1(self.previouspagerank, self.pagerank)
        self.errors.append(self.error)

    def save(self):
        '''
        Save computed PageRank vector to a local file.
        '''
        self.dumpPageRankVector()

    def dumpPageRankVector(self):
        from webmatrix.util import DoubleArrays
        data = self.pagerank.getData()
        DoubleArrays.dump(data, self.pagerankvectorpath)


    def init(self):
        pass




class CompressedWebGraphRanker(SequentialRanker):
    '''
    A sequential algorithm (single computing flow) for ranking;
    (compressed) webgraph formatted representation of link structure is used.
    '''
    def __init__(self, parameters):
        self.setupParameters(parameters)
        self.loadGraph()
        self.loadPreferencesVector()
        self.loadInitialPageRank()
        self.init()

        self.iterations = 0
        self.errors = []
        self.error = self.threshold + 1.0


    def setupParameters(self, parameters):
        '''
        Import computing parameters as attributes of the object.

        @param parameters:computing parameters
        '''

        graphpath = parameters['graphpath']
        initialpagerankvectorpath = parameters.get('initialpagerankvectorpath', None)
        preferencesvectorpath = parameters.get('preferencesvectorpath', None)
        pagerankvectorpath = parameters['pagerankvectorpath']
        alpha = parameters.get('alpha', 0.85)
        threshold = parameters.get('threshold', 1e-4)
        maxiterations = parameters.get('maxiterations', 30)
        stopmethod = parameters.get('stopmethod', 'IterationNumber') # the other being 'NormDelta'
        computemethod = parameters.get('computemethod', 'PowerMethod')

        self.graphpath = graphpath
        self.initialpagerankvectorpath = initialpagerankvectorpath
        self.preferencesvectorpath = preferencesvectorpath
        self.pagerankvectorpath = pagerankvectorpath
        self.alpha = alpha
        self.threshold = threshold
        self.maxiterations = maxiterations
        self.stopmethod = stopmethod
        self.computemethod = computemethod


    def loadGraph(self):
        '''
        Loads graph as stored in a file.
        '''
        from it.unimi.dsi.webgraph import BVGraph
        graph = BVGraph.loadOffline(self.graphpath)
        n = graph.numNodes()
        self.graph = graph
        self.n = n


    def loadPreferencesVector(self):
        '''
        Loads preferences vector as stored in a file.
        '''
        from webmatrix.util import DoubleArrays
        from it.unimi.dsi.fastutil.doubles import DoubleArrayList
        if self.preferencesvectorpath is not None:
            datamatrix =  DoubleArrays.load(self.preferencesvectorpath)
            data = DoubleArrays.flatten(datamatrix)
        else:
            value = 1.0 / self.n
            data = DoubleArrays.repeat(value, self.n)
        preferencesvector = DoubleArrayList(data)
        self.preferencesvector = preferencesvector


    def loadInitialPageRank(self):
        '''
        Loads initial PageRank vector as stored in a file.
        '''
        from webmatrix.util import DoubleArrays
        from it.unimi.dsi.fastutil.doubles import DoubleArrayList
        if self.initialpagerankvectorpath is not None:
            datamatrix =  DoubleArrays.load(self.initialpagerankvectorpath)
            data = DoubleArrays.flatten(datamatrix)
        else:
            value = 1.0 / self.n
            data = DoubleArrays.repeat(value, self.n)
        initialpagerank = DoubleArrayList(data)
        self.initialpagerank = initialpagerank


    def init(self):
        self.initRanker()


    def initRanker(self):
        from it.unimi.dsi.law.rank import *
        if self.computemethod == 'PowerMethod':
            self.ranker = PageRankPowerMethod(self.graph)
        elif  self.computemethod == 'Jacobi':
            self.ranker = PageRankPowerMethod(self.graph)
        elif self.computemethod == 'GaussSeidel':
            from it.unimi.dsi.webgraph import Transform
            batchSize = self.n
            self.graph = Transform.transposeOffline(graph, batchSize)
            self.ranker = PageRankPowerMethod(self.graph)

        self.ranker.start = self.initialpagerank
        self.ranker.preference = self.preferencesvector
        self.ranker.alpha = self.alpha

        if (self.stopmethod == 'IterationNumber'):
            rankerStopCheck = PageRank.IterationNumberStoppingCriterion(self.maxiterations)
        elif (self.stopmethod == 'NormDelta'):
            rankerStopCheck = PageRank.NormDeltaStoppingCriterion(self.threshold)
        self.rankerStopCheck = rankerStopCheck



    def compute(self):
        '''
        Compute (i.e. step again and again)
        until convergence condition is satisfied.
        '''
        self.ranker.init()
        while not self.converged():
            self.ranker.step()
            self.updateError()
            self.iterations = self.iterations + 1
        # self.ranker.stepUntil(self.rankerStopCheck)


    def step(self):
        self.ranker.step()


    def converged(self):
        return self.rankerStopCheck.shouldStop(self.ranker)


    def updateError(self):
        self.error = self.ranker.normDelta()
        self.errors.append(self.error)


    def save(self):
        '''
        Save computed PageRank vector to a local file.
        '''
        self.dumpPageRankVector()


    def dumpPageRankVector(self):
        from webmatrix.util import DoubleArrays
        DoubleArrays.dump(self.ranker.rank, self.pagerankvectorpath)



class ThreadedRanker(Thread, ConcurrentRanker, ThreadedIterativeAlgorithm):
    '''
    Concurrent iterative algorithm for ranking; one of the
    computing flows implemented as one of multiple threads within a
    single process.
    '''
    def __init__(self, communicator, parameters):
        Thread.__init__(self)
        self.communicator = communicator
        self.setupParameters(parameters)
        self.loadGraphlet()
        self.loadPreferencesVector()
        self.loadInitialPageRank()
        self.init()

        self.iterations = 0
        self.errors = []
        self.error = self.threshold + 1.0


    def setupParameters(self, parameters):
        self.rankerID = parameters['rankerID']
        self.size = parameters['size']
        self.graphletpath = parameters['graphletpath']
        self.initialpagerankvectorpath = parameters.get('initialpagerankvectorpath', None)
        self.preferencesvectorpath = parameters.get('preferencesvectorpath', None)
        self.pagerankvectorpath = parameters['pagerankvectorpath']
        self.alpha = parameters.get('alpha', 0.85)
        self.startdelay = parameters.get('startdelay', None)
        self.stepdelay = parameters.get('stepdelay', None)
        self.threshold = parameters.get('threshold', 1e-4)
        self.maxiterations = parameters.get('maxiterations', 30)
        self.stopmethod = parameters.get('stopmethod', 'IterationNumber') # the other being 'NormDelta'


    def loadGraphlet(self):
        '''
        Load graph part (graphlet) as stored in a file.
        '''
        graphlet = Graphlet.load(self.graphletpath)
        n = graphlet.getN()
        self.graphlet = graphlet
        self.n = n


    def loadPreferencesVector(self):
        from webmatrix.util import DoubleArrays
        if self.preferencesvectorpath is not None:
            datamatrix =  DoubleArrays.load(self.preferencesvectorpath)
            preferencesvector = DoubleArrays.flatten(datamatrix)
        else:
            value = 1.0 / self.n
            preferencesvector = DoubleArrays.repeat(value, self.n)
        self.preferencesvector = preferencesvector


    def loadInitialPageRank(self):
        if self.initialpagerankvectorpath is not None:
            from webmatrix.util import DoubleArrays
            from webmatrix import PageRank
            datamatrix = DoubleArrays.load(self.initialpagerankvectorpath)
            initialpagerankvector = DoubleArrays.flatten(datamatrix)
            initialpagerank = PageRank(initialpagerankvector)
        else:
            from webmatrix import Ranks
            initialpagerank = Ranks.newUniformRank(self.n)
        self.initialpagerank = initialpagerank
        self.pagerank = initialpagerank


    def compute(self):
        if self.startdelay is not None:
            time.sleep(self.startdelay)
        self.assemble()
        while not self.converged():
            if self.stepdelay is not None:
                time.sleep(self.stepdelay)
            self.step()
            self.distribute()
            self.assemble()
            self.updateError()
            self.iterations = self.iterations + 1
            print '%d: %f' % (self.rankerID, self.error) #XXX
        #XXX print '#%d awaiting' % (self.rankerID,) #XXX


    def step(self):
        self.previouspagerank = self.pagerank
        self.ranklet = self.graphlet.mult(self.pagerank, self.alpha, self.preferencesvector)



    def assemble(self):
        '''
        Construct a local PageRank vector from locally available
        (received or computed) rank parts (ranklets).
        '''
        self.previouspagerank = self.pagerank
        ranklets = []
        for i in range(self.size):
            name = '#%d' % i
            box = self.communicator.inboxes[name]
            #XXX print box #XXX
            ranklet = box.get()
            #XXX print ranklet #XXX
            #XXX print 'ranklet size: %d' % ranklet.getSize() #XXX
            ranklets.append(ranklet)
        pagerank = PageRank(Ranklet.pack(ranklets))
        self.pagerank = pagerank


    def distribute(self):
        name = '#%d' % self.rankerID
        box = self.communicator.inboxes[name]
        box.set(self.ranklet)


    def converged(self):
        if (self.stopmethod == 'IterationNumber'):
            if (self.iterations > self.maxiterations):
                return True
            return False
        elif (self.stopmethod == 'NormDelta'):
            if (self.error < self.threshold and self.iterations > 0):
                return True
            return False


    def updateError(self):
        self.error = Ranks.diffNorm1(self.previouspagerank, self.pagerank)
        self.errors.append(self.error)

    def init(self):
        #XXX print 'ThreadedRanker initialized()' #XXX
        pass

    def run(self):
        #XXX print 'ThreadedRanker running' #XXX
        self.compute()


    def save(self):
        self.dumpPageRankVector()


    def dumpPageRankVector(self):
        from webmatrix.util import DoubleArrays
        data = self.pagerank.getData()
        DoubleArrays.dump(data, self.pagerankvectorpath)



class ParallelRanker(Thread, ConcurrentRanker, ParallelIterativeAlgorithm):
    '''
    Concurrent iterative algorithm for ranking; one of the
    computing flows implemented as one of multiple threads, each in its
    own process.
    '''
    def __init__(self, communicator, parameters):
        Thread.__init__(self)
        self.communicator = communicator

        self.setupParameters(parameters)
        self.loadGraphlet()
        self.loadPreferencesVector()
        self.loadInitialPageRank()
        self.init()


        self.iterations = 0
        self.errors = []
        self.error = self.threshold + 1.0



    def setupParameters(self, parameters):
        self.rankerID = parameters['rankerID']
        self.size = parameters['size']
        self.graphletpath = parameters['graphletpath']
        self.initialpagerankvectorpath = parameters.get('initialpagerankvectorpath', None)
        self.preferencesvectorpath = parameters.get('preferencesvectorpath', None)
        self.pagerankvectorpath = parameters['pagerankvectorpath']
        self.alpha = parameters.get('alpha', 0.85)
        self.threshold = parameters.get('threshold', 1e-4)
        self.maxiterations = parameters.get('maxiterations', 30)
        self.stopmethod = parameters.get('stopmethod', 'IterationNumber') # the other being 'NormDelta'



    def loadGraphlet(self):
        graphlet = Graphlet.load(self.graphletpath)
        n = graphlet.getN()
        self.graphlet = graphlet
        self.n = n


    def loadPreferencesVector(self):
        from webmatrix.util import DoubleArrays
        if self.preferencesvectorpath is not None:
            datamatrix =  DoubleArrays.load(self.preferencesvectorpath)
            preferencesvector = DoubleArrays.flatten(datamatrix)
        else:
            value = 1.0 / self.n
            preferencesvector = DoubleArrays.repeat(value, self.n)
        self.preferencesvector = preferencesvector


    def loadInitialPageRank(self):
        if self.initialpagerankvectorpath is not None:
            from webmatrix.util import DoubleArrays
            from webmatrix import PageRank
            datamatrix = DoubleArrays.load(self.initialpagerankvectorpath)
            initialpagerankvector = DoubleArrays.flatten(datamatrix)
            initialpagerank = PageRank(initialpagerankvector)
        else:
            from webmatrix import Ranks
            initialpagerank = Ranks.newUniformRank(self.n)
        self.initialpagerank = initialpagerank
        self.pagerank = initialpagerank


    def compute(self):
        self.assemble()
        while not self.converged():
            self.step()
            self.distribute()
            self.assemble()
            self.updateError()
            self.iterations = self.iterations + 1
            print '%d: %f' % (self.rankerID, self.error) #XXX
        # print '#%d awaiting' % (self.rankerID,)


    def step(self):
        self.previouspagerank = self.pagerank
        self.ranklet = self.graphlet.mult(self.pagerank, self.alpha, self.preferencesvector)


    def assemble(self):
        ranklets = []
        for i in range(self.size):
            name = '#%d <- #%d' % (self.rankerID, i)
            box = self.communicator.inboxes[name]
            ranklet = box.get()
            ranklets.append(ranklet)
        pagerank = PageRank(Ranklet.pack(ranklets))
        self.pagerank = pagerank



    def distribute(self):
        for i in range(self.size):
            if i != self.rankerID:
                name = '#%d -> #%d' % (self.rankerID, i)
                box = self.communicator.outboxes[name]
                box.set(self.ranklet)
        name = '#%d <- #%d' % (self.rankerID, self.rankerID)
        box = self.communicator.inboxes[name]
        box.set(self.ranklet)



    def converged(self):
        if (self.stopmethod == 'IterationNumber'):
            if (self.iterations > self.maxiterations):
                return True
            return False
        elif (self.stopmethod == 'NormDelta'):
            if (self.error < self.threshold and self.iterations > 0):
                return True
            return False


    def updateError(self):
        self.error = Ranks.diffNorm1(self.previouspagerank, self.pagerank)
        self.errors.append(self.error)


    def init(self):
        ranklets = self.pagerank.split(self.size)
        self.communicator.init(ranklets)


    def run(self):
        self.communicator.start()
        self.compute()


    def save(self):
        self.dumpPageRankVector()


    def dumpPageRankVector(self):
        from webmatrix.util import DoubleArrays
        data = self.pagerank.getData()
        DoubleArrays.dump(data, self.pagerankvectorpath)



########################################################################
# Computation
########################################################################

class Computation:
    '''
    Manager of a local computing thread.
    '''
    def processConfiguration(self, configuration):
        pass

    def processProperties(self, properties):
        pass



class SequentialComputation(Computation):
    '''
    Manager of the (single) computing flow in the computation.
    '''
    pass

class ConcurrentComputation(Computation):
    '''
    Manager of one of the multiple computing flows making up the
    computation.
    '''
    pass



class ThreadedComputation(ConcurrentComputation):
    '''
    Manager of one of the multiple computing threads executing
    within the context of the computation, in a single process.
    '''
    pass



class ParallelComputation(ConcurrentComputation):
    '''
    Manager of one of the multiple computing threads executing
    within the context of the computation, implemented as a set of
    processes, possibly distributed over multiple machines.
    '''
    def processCapabilities(self, capabilities):
        pass


class SequentialRank(SequentialComputation):
    '''
    Manager of a sequential computation of PageRank.
    '''
    def setupConfiguration(self, configuration):
        self.inbaselocator = configuration['inbaselocator']
        self.outbaselocator = configuration['outbaselocator']
        self.initialpagerankvectorQ = configuration.get('initialpagerankvectorQ', False)
        self.preferencesvectorQ = configuration.get('preferencesvectorQ', False)
        self.alpha = configuration.get('alpha', 0.85)

        self.threshold = configuration.get('threshold', 1e-4)
        self.maxiterations = configuration.get('maxiterations', 30)
        self.stopmethod = configuration.get('stopmethod', 'IterationNumber')


    def setupParameters(self):
        parameters = {}
        parameters['graphpath'] = self.localgraphpath
        if self.initialpagerankvectorQ:
            parameters['initialpagerankvectorpath'] = self.localinitialpagerankvectorpath
        if self.preferencesvectorQ:
            parameters['preferencesvectorpath'] = self.localpreferencesvectorpath
        parameters['pagerankvectorpath'] = self.localpagerankvectorpath
        parameters['alpha'] = self.alpha
        parameters['threshold'] = self.threshold
        parameters['maxiterations'] = self.maxiterations
        parameters['stopmethod'] = self.stopmethod
        self.parameters = parameters


    def download(self):
        '''
        Download necessary remote files either available through a web server,
        or hosted in a Storage Element (SE).
        '''
        self.inutils = None
        inbasescheme = self.getLocatorScheme(self.inbaselocator)
        if inbasescheme == 'http':
            inutils = WebUtils()
        elif inbasescheme == 'lfn':
            inutils = GridUtils()
        if inbasescheme == 'http' or inbasescheme == 'lfn':
            if self.initialpagerankvectorQ:
                if not os.path.exists(self.localinitialpagerankvectorpath):
                    inutils.download(self.remoteinitialpagerankvectorpath, self.localinitialpagerankvectorpath)
            if self.preferencesvectorQ:
                if not os.path.exists(self.localpreferencesvectorpath):
                    inutils.download(self.remotepreferencesvectorpath, self.localpreferencesvectorpath)
            self.inutils = inutils


    def upload(self):
        '''
        Upload computation produced files to a Storage Element (SE).
        '''

        self.oututils = None
        outbasescheme = self.getLocatorScheme(self.outbaselocator)
        if outbasescheme == 'lfn':
            oututils = GridUtils()
            oututils.upload(self.localpagerankvectorpath, self.remotepagerankvectorpath)
            self.oututils = oututils


    def getLocalBasePath(self, locator):
        import urlparse
        import os
        (scheme, location, path, query, fragment) = urlparse.urlsplit(locator)
        if scheme == 'file' or scheme == '':
            localpath = path
        else:
            filename = os.path.basename(path)
            localpath = os.path.abspath(filename)
        return localpath


    def getLocatorScheme(self, locator):
        import urlparse
        import os
        (scheme, location, path, query, fragment) = urlparse.urlsplit(locator)
        if scheme == 'file' or scheme =='':
            return 'file'
        elif scheme == 'http':
            return 'http'
        else:
            return 'lfn'


class ConcurrentRank(ConcurrentComputation):
    '''
    Manager of a concurrent implementation of PageRank.
    '''

    def setupConfiguration(self, configuration):
        self.inbaselocator = configuration['inbaselocator']
        self.outbaselocator = configuration['outbaselocator']
        self.initialpagerankvectorQ = configuration.get('initialpagerankvectorQ', False)
        self.preferencesvectorQ = configuration.get('preferencesvectorQ', False)
        self.alpha = configuration.get('alpha', 0.85)

        self.threshold = configuration.get('threshold', 1e-4)
        self.maxiterations = configuration.get('maxiterations', 30)
        self.stopmethod = configuration.get('stopmethod', 'IterationNumber')



    def setupLocalPaths(self):
        inlocalbasepath = self.getLocalBasePath(self.inbaselocator)
        outlocalbasepath = self.getLocalBasePath(self.outbaselocator)
        self.localinitialpagerankvectorpath = '%s-%s.bin' % (inlocalbasepath, 'initialpagerankvector')
        self.localpreferencesvectorpath = '%s-%s.bin' % (inlocalbasepath, 'preferencesvector')

    def setupRemotePaths(self):
        self.remoteinitialpagerankvectorpath = '%s-%s.bin' % (self.inbaselocator, 'initialpagerankvector')
        self.remotepreferencesvectorpath = '%s-%s.bin' % (self.inbaselocator, 'preferencesvector')

    def download(self):
        self.inutils = None
        inbasescheme = self.getLocatorScheme(self.inbaselocator)
        if inbasescheme == 'http':
            inutils = WebUtils()
        elif inbasescheme == 'lfn':
            inutils = GridUtils()
        if inbasescheme == 'http' or inbasescheme == 'lfn':
            if self.initialpagerankvectorQ:
                if not os.path.exists(self.localinitialpagerankvectorpath):
                    inutils.download(self.remoteinitialpagerankvectorpath, self.localinitialpagerankvectorpath)
            if self.preferencesvectorQ:
                if not os.path.exists(self.localpreferencesvectorpath):
                    inutils.download(self.remotepreferencesvectorpath, self.localpreferencesvectorpath)
            self.inutils = inutils


    def upload(self):
        self.oututils = None
        outbasescheme = self.getLocatorScheme(self.outbaselocator)
        if outbasescheme == 'lfn':
            oututils = GridUtils()
            oututils.upload(self.localpagerankvectorpath, self.remotepagerankvectorpath)
            self.oututils = oututils


    def getLocalBasePath(self, locator):
        import urlparse
        import os
        (scheme, location, path, query, fragment) = urlparse.urlsplit(locator)
        if scheme == 'file' or scheme == '':
            localpath = path
        else:
            filename = os.path.basename(path)
            localpath = os.path.abspath(filename)
        return localpath


    def getLocatorScheme(self, locator):
        import urlparse
        import os
        (scheme, location, path, query, fragment) = urlparse.urlsplit(locator)
        if scheme == 'file' or scheme =='':
            return 'file'
        elif scheme == 'http':
            return 'http'
        else:
            return 'lfn'



class WebMatrixRank(SequentialRank):
    '''
    Manager of a sequential PageRank computation using
    webmatrix format for the link structure.
    '''

    def __init__(self, properties, configuration):
        self.setupProperties(properties)
        self.processConfiguration(configuration)
        self.init()


    def setupProperties(self, properties):
        pass



    def processConfiguration(self, configuration):
        self.setupConfiguration(configuration)
        self.setupPaths()
        self.setupParameters()


    def setupConfiguration(self, configuration):
        SequentialRank.setupConfiguration(self, configuration)



    def setupPaths(self):
        self.setupLocalPaths()
        self.setupRemotePaths()


    def setupLocalPaths(self):
        inlocalbasepath = self.getLocalBasePath(self.inbaselocator)
        outlocalbasepath = self.getLocalBasePath(self.outbaselocator)
        self.localgraphpath = '%s.bin' % inlocalbasepath
        self.localpagerankvectorpath = '%s-%s.bin' % (outlocalbasepath, 'pagerankvector')
        self.localinitialpagerankvectorpath = '%s-%s.bin' % (inlocalbasepath, 'initialpagerankvector')
        self.localpreferencesvectorpath = '%s-%s.bin' % (inlocalbasepath, 'preferencesvector')


    def setupRemotePaths(self):
        self.remotegraphpath = '%s.bin' % self.inbaselocator
        self.remotepagerankvectorpath = '%s-%s.bin' % (self.outbaselocator, 'pagerankvector')
        self.remoteinitialpagerankvectorpath = '%s-%s.bin' % (self.inbaselocator, 'initialpagerankvector')
        self.remotepreferencesvectorpath = '%s-%s.bin' % (self.inbaselocator, 'preferencesvector')


    def setupParameters(self):
        SequentialRank.setupParameters(self)


    def download(self):
        SequentialRank.download(self)
        if self.inutils is not None:
            self.inutils.download(self.remotegraphpath, self.localgraphpath)

    def init(self):
        '''
        Downloads necessary files and initializes managed computing thread.
        '''
        self.download()
        self.initRanker()


    def initRanker(self):
        '''
        Initializes managed computing thread.
        '''
        ranker = WebMatrixRanker(self.parameters)
        self.ranker = ranker


    def execute(self):
        '''
        Computes, saves results locally and possibly uploads them
        to a remote location.
        '''
        self.ranker.compute()
        self.ranker.save()
        self.upload()



class CompressedWebGraphRank(SequentialRank):
    '''
    Manager of a sequential PageRank computation using
    (compressed) webgraph format for the link structure.
    '''
    def __init__(self, properties, configuration):
        self.setupProperties(properties)
        self.processConfiguration(configuration)
        self.init()


    def setupProperties(self, properties):
        pass



    def processConfiguration(self, configuration):
        self.setupConfiguration(configuration)
        self.setupPaths()
        self.setupParameters()


    def setupConfiguration(self, configuration):
        SequentialRank.setupConfiguration(self, configuration)


    def setupPaths(self):
        self.setupLocalPaths()
        self.setupRemotePaths()


    def setupLocalPaths(self):
        inlocalbasepath = self.getLocalBasePath(self.inbaselocator)
        outlocalbasepath = self.getLocalBasePath(self.outbaselocator)
        self.localgraphpath = '%s' % inlocalbasepath
        self.localpagerankvectorpath = '%s-%s.bin' % (outlocalbasepath, 'pagerankvector')
        self.localinitialpagerankvectorpath = '%s-%s.bin' % (inlocalbasepath, 'initialpagerankvector')
        self.localpreferencesvectorpath = '%s-%s.bin' % (inlocalbasepath, 'preferencesvector')


    def setupRemotePaths(self):
        self.remotegraphpath = '%s.tar.gz' % self.inbaselocator
        self.remotepagerankvectorpath = '%s-%s.bin' % (self.outbaselocator, 'pagerankvector')
        self.remoteinitialpagerankvectorpath = '%s-%s.bin' % (self.inbaselocator, 'initialpagerankvector')
        self.remotepreferencesvectorpath = '%s-%s.bin' % (self.inbaselocator, 'preferencesvector')


    def setupParameters(self):
        SequentialRank.setupParameters(self)

    def download(self):
        SequentialRank.download(self)
        if self.inutils is not None:
            self.inutils.download('%s.tar.gz' % self.remotegraphpath, '%s.tar.gz' % self.localgraphpath)
            host = HostUtils()
            host.extract('%s.tar.gz' % self.localgraphpath, os.path.dirname(self.localgraphpath))


    def init(self):
        self.download()
        self.initRanker()


    def initRanker(self):
        ranker = CompressedWebGraphRanker(self.parameters)
        self.ranker = ranker


    def execute(self):
        self.ranker.compute()
        self.ranker.save()
        self.upload()



class ThreadedRank(ConcurrentRank, ThreadedComputation):
    '''
    Manager of a concurrent computation of PageRank, implemented as multiple
    threads within a single process.
    '''
    def __init__(self, properties, configuration, mode='asynchronous'):
        self.communicator = ThreadRankletCommunicator(properties, mode)
        self.setupProperties(properties)
        self.processConfiguration(configuration)
        self.init()


    def setupProperties(self, properties):
        size = properties['size']
        self.size = size



    def processConfiguration(self, configuration):
        self.setupConfiguration(configuration)
        self.setupPaths()
        self.setupParametersList()


    def setupConfiguration(self, configuration):
        ConcurrentRank.setupConfiguration(self, configuration)

        startdelays = configuration.get('startdelays', None)
        if startdelays is None:
            startdelays = [None for i in range(self.size)]
        self.startdelays = startdelays

        stepdelays = configuration.get('stepdelays', None)
        if stepdelays is None:
            stepdelays = [None for i in range(self.size)]
        self.stepdelays = stepdelays



    def setupPaths(self):
        self.setupLocalPaths()
        self.setupRemotePaths()


    def setupLocalPaths(self):
        ConcurrentRank.setupLocalPaths(self)

        inlocalbasepath = self.getLocalBasePath(self.inbaselocator)
        outlocalbasepath = self.getLocalBasePath(self.outbaselocator)
        self.localpagerankvectorpaths = []
        self.localgraphletpaths = []
        for i in range(self.size):
            graphletpath = '%s-%d-%d.bin' % (inlocalbasepath, self.size, i)
            pagerankvectorpath = '%s-%s-%d-%d.bin' % (outlocalbasepath, 'pagerankvector', self.size, i)
            self.localgraphletpaths.append(graphletpath)
            self.localpagerankvectorpaths.append(pagerankvectorpath)


    def setupRemotePaths(self):
        ConcurrentRank.setupRemotePaths(self)

        self.remotepagerankvectorpaths = []
        self.remotegraphletpaths = []
        for i in range(self.size):
            graphletpath = '%s-%d-%d.bin' % (self.inbaselocator, self.size, i)
            pagerankvectorpath = '%s-%s-%d-%d.bin' % (self.outbaselocator, 'pagerankvector', self.size, i)
            self.remotegraphletpaths.append(graphletpath)
            self.remotepagerankvectorpaths.append(pagerankvectorpath)


    def getParameters(self, rankerID):
        parameters = {}
        parameters['rankerID'] = rankerID
        parameters['size'] = self.size
        parameters['graphletpath'] = self.localgraphletpaths[rankerID]
        if self.initialpagerankvectorQ:
            parameters['initialpagerankvectorpath'] = self.localinitialpagerankvectorpath
        if self.preferencesvectorQ:
            parameters['preferencesvectorpath'] = self.localpreferencesvectorpath
        parameters['pagerankvectorpath'] = self.localpagerankvectorpaths[rankerID]
        parameters['alpha'] = self.alpha
        parameters['threshold'] = self.threshold
        parameters['maxiterations'] = self.maxiterations
        parameters['stopmethod'] = self.stopmethod
        return parameters


    def setupParametersList(self):
        self.parameterslist = []
        for i in range(self.size):
            parameters = self.getParameters(i)
            self.parameterslist.append(parameters)

    def loadInitialPageRank(self):
        aranker = self.communicator.rankers['#%d' % 0]
        initialpagerankvectorpath = aranker.initialpagerankvectorpath
        n = aranker.n
        if initialpagerankvectorpath is not None:
            from webmatrix.util import DoubleArrays
            from webmatrix import PageRank
            datamatrix = DoubleArrays.load(initialpagerankvectorpath)
            initialpagerankvector = DoubleArrays.flatten(datamatrix)
            initialpagerank = PageRank(initialpagerankvector)
        else:
            from webmatrix import Ranks
            initialpagerank = Ranks.newUniformRank(n)
        self.initialpagerank = initialpagerank


    def init(self):
        self.download()
        self.communicator.initRankers(self.parameterslist)

        self.loadInitialPageRank()
        ranklets = self.initialpagerank.split(self.size)
        self.communicator.initBoxes(ranklets)
        #XXX for i in range(self.size):#XXX
        #XXX    print 'during init() ranklets at ThreadRank: %d' % ranklets[i].getSize()#XXX


    def execute(self):
        '''
        Start computing across all computing threads and after the computation
        completes (posssibly) upload result files to a remote location.
        '''
        self.communicator.startRankers()
        self.upload()


class IbisParallelRank(ConcurrentRank, ParallelComputation):
    '''
    Manager of a concurrent computation of PageRank,
    finally implemented as multiple threads scattered across
    multiple processes.

    The manager manages only the local computing thread.
    '''
    def __init__(self, properties, capabilities, configuration, mode='asynchronous'):
        self.communicator = IbisRankletCommunicator(properties, capabilities, mode)
        self.setupProperties(properties)
        self.processConfiguration(configuration)
        self.init()


    def setupProperties(self, properties):
        size = int(self.communicator.size)
        self.size = size



    def processConfiguration(self, configuration):
        self.setupConfiguration(configuration)
        self.setupPaths()
        self.setupParameters()


    def setupConfiguration(self, configuration):
        ConcurrentRank.setupConfiguration(self, configuration)


    def setupPaths(self):
        self.setupLocalPaths()
        self.setupRemotePaths()


    def setupLocalPaths(self):
        rankerID = self.communicator.rankID
        inlocalbasepath = self.getLocalBasePath(self.inbaselocator)
        outlocalbasepath = self.getLocalBasePath(self.outbaselocator)
        self.localgraphletpath = '%s-%d-%d.bin' % (inlocalbasepath, self.size, rankerID)
        self.localpagerankvectorpath = '%s-%s-%d-%d.bin' % (outlocalbasepath, 'pagerankvector', self.size, rankerID)
        self.localinitialpagerankvectorpath = '%s-%s.bin' % (inlocalbasepath, 'initialpagerankvector')
        self.localpreferencesvectorpath = '%s-%s.bin' % (inlocalbasepath, 'preferencesvector')


    def setupRemotePaths(self):
        rankerID = self.communicator.rankID
        self.remotegraphletpath = '%s-%d-%d.bin' % (self.inbaselocator, self.size, rankerID)
        self.remotepagerankvectorpath = '%s-%s-%d-%d.bin' % (self.outbaselocator, 'pagerankvector', self.size, rankerID)
        self.remoteinitialpagerankvectorpath = '%s-%s.bin' % (self.inbaselocator, 'initialpagerank')
        self.remotepreferencesvectorpath = '%s-%s.bin' % (self.inbaselocator, 'preferencesvector')


    def setupParameters(self):
        parameters = {}
        rankerID = self.communicator.rankID
        parameters['rankerID'] = rankerID
        parameters['size'] = self.size
        parameters['graphletpath'] = self.localgraphletpath
        if self.initialpagerankvectorQ:
            parameters['initialpagerankvectorpath'] = self.localinitialpagerankvectorpath
        if self.preferencesvectorQ:
            parameters['preferencesvectorpath'] = self.localpreferencesvectorpath
        parameters['pagerankvectorpath'] = self.localpagerankvectorpath
        parameters['alpha'] = self.alpha
        parameters['threshold'] = self.threshold
        parameters['maxiterations'] = self.maxiterations
        parameters['stopmethod'] = self.stopmethod
        self.parameters = parameters



    def download(self):
        ConcurrentRank.download(self)
        if self.inutils is not None:
            self.inutils.download(self.remotegraphletpath, self.localgraphletpath)


    def init(self):
        self.download()
        self.initRanker()


    def initRanker(self):
        ranker = ParallelRanker(self.communicator, self.parameters)
        self.ranker = ranker


    def execute(self):
        self.ranker.start()
        self.upload()


########################################################################
# Experiment
########################################################################

class Experiment:
    '''
    Abstraction of the target platform where actual experimentation takes
    place.

    '''
    def installRuntime(self):
        pass

    def prepareData(self):
        pass

    def setupComputation(self):
        pass


    def start(self):
        pass

    def execute(self):
        pass

    def clear(self):
        pass



class LocalExperiment(Experiment):
    '''
    Experiment deployed in a single machine.
    '''
    def __init__(self, description):
        pass



class ClusterExperiment(Experiment):
    '''
    Experiment deployed across a cluster of machines.

    Machines making up the cluster might be nodes in LAN or connected
    through generic Internet connections (WAN)
    '''
    # assume ssh for all data moves
    def __init__(self, description):
        pass




class GridExperiment(Experiment):
    '''
    Experiment finally deployed across one or multiple Worker Nodes (WNs)
    within the Grid platform (as defined by EGEE)
    '''
    def __init__(self, description):
        pass

    def setupComputation(self):
        self.setupJobDescriptions()

    def setupJobDescriptions(self):
        # using description, construct Job objects using jylab.grid
        pass

    def start(self):
        # + submit job descriptions using jylab.grid
        pass

    def execute(self):
        self.start()
        # + download results data using jylab.grid



########################################################################
# DataGenerator
########################################################################

class DataGenerator:
    '''
    An auxiliary class for generating (synthetic) test data.
    '''
    pass


class WebMatrixGenerator(DataGenerator):
    '''
    Generator of synthetic link structures in the webmatrix format.
    '''
    def __init__(self, kind, n, outdegree, outdirpath='.', blocks=10, prob=0.8):
        '''
        Constructs the synthetic link structure generator (webmatrix format).

        @param kind: one of the strings: "barabasi-albert",
        "ncd-random", "random" selecting the internal algorithm used in
        the generation
        @param n: the size of the generated matrix (nxn)
        @param outdegree: how many outlinks will each node have?
        @param outdirpath: the top directory for saving the file with the
        webmatrix
        @param blocks: how many blocks to build for the "ncd-random"
        generator?
        @param prob: the probability a newly generated node to belong
        to one of the blocks across matrix diagonal ("ncd-random" case)
        '''

        self.kind = kind
        self.outdirpath = outdirpath
        self.n = n
        self.outdegree = outdegree
        self.blocks = blocks
        self.prob = prob


    def generateBarabasiAlbertGraph(self, n, outdegree):
        graph = Graphs.newBarabasiAlbertGraph(n, outdegree)
        filename = '%s-%d.bin' % (self.kind, n)
        filepath = os.path.join(os.path.abspath(self.outdirpath), filename)
        graph.dump(filepath)
        self.filepath = filepath


    def generateNCDRandomGraph(self, n, outdegree, blocks, prob=0.8):
        graph = Graphs.newNCDRandomGraph(n, outdegree, blocks, prob)
        filename = '%s-%d.bin' % (self.kind, n)
        filepath = os.path.join(os.path.abspath(self.outdirpath), filename)
        graph.dump(filepath)
        self.filepath = filepath


    def generateRandomGraph(self, n, outdegree):
        graph = Graphs.newRandomGraph(n, outdegree)
        filename = '%s-%d.bin' % (self.kind, n)
        filepath = os.path.join(os.path.abspath(self.outdirpath), filename)
        graph.dump(filepath)
        self.filepath = filepath

    def generate(self):
        '''
        Generate and save the synthetic webmatrix.
        '''
        if self.kind == 'barabasi-albert':
            self.generateBarabasiAlbertGraph(self.n, self.outdegree)
        elif self.kind == 'ncd-random':
            self.generateBarabasiAlbertGraph(self.n, self.outdegree, self.blocks, self.prob)
        elif self.kind == 'random':
            self.generateBarabasiAlbertGraph(self.n, self.outdegree)




########################################################################
# DataPartitioner
########################################################################

class DataPartitioner:

    def split(self):
        pass

    def dump(self):
        pass

    def clear(self):
        pass




class WebMatrixPartitioner(DataPartitioner):
    '''
    Auxiliary class containing classes for partitioning and saving a
    link structure in webmatrix format.
    '''
    def __init__(self, infilepath, m, outdirpath='.'):
        '''
        Construct the webmatrix partitioner.

        @param infilepath: the local file where the webmatrix is initially
        stored
        @param m: how many parts to split the webmatrix into
        @param outdirpath: the local topdir for saving the webmatrix parts
        '''

        self.infilepath = infilepath
        self.m = m
        self.outdirpath = outdirpath

        [filebase, extension] = os.path.basename(infilepath).split('.')
        basename = os.path.join(outdirpath, filebase)
        self.basename = os.path.abspath(basename)

        outfilepaths = self.__paths()
        partitioned = False
        fpath = outfilepaths[0]
        if (os.path.exists(fpath)):
            partitioned = True

        if (not partitioned):
            self.graph = WebGraph.load(infilepath)
            self.graphlets = None

        self.outfilepaths = outfilepaths
        self.partitioned = partitioned


    def split(self):
        '''
        Partition the webmatrix (do the splitting)
        '''
        if (not self.partitioned):
            graphlets = self.graph.split(self.m)
            self.graphlets = graphlets
            print 'split(): OK'


    def dump(self):
        '''
        Save the parts of the partition, i.e. put graphlets into
        local files.
        '''
        if (not self.partitioned):
            if (self.graphlets is None):
                self.split()
            for i in range(self.m):
                graphlet = self.graphlets[i]
                fpath = self.outfilepaths[i]
                graphlet.dump(fpath)
            self.partitioned = True
            print 'dump(): OK'


    def clear(self):
        '''
        Remove the graphlets (from the partitioned graph)
        '''
        try:
            for fpath in self.outfilepaths:
                os.remove(fpath)
            self.partitioned = False
            print 'clear(): OK'
        except:
            print 'clear() : Exception'


    def __paths(self):
        import os
        fpaths = []
        for i in range(self.m):
            fname = '%s-%d-%d.bin' % (self.basename, self.m, i)
            fpath = os.path.join(os.path.abspath(self.outdirpath), fname)
            fpaths.append(fpath)
        return fpaths


########################################################################
# HostUtils
########################################################################

class HostUtils:
    '''
    A container of file related utilities
    '''
    def mkdir(self, dname):
        template = 'mkdir -p %s'
        cmd = template % dname
        os.system(cmd)

    def rmdir(self, dname, force=False):
        if not force:
            template = 'rmdir -f %s'
        else:
            template = 'rmdir -rf %s'

        cmd = template % dname
        os.system(cmd)


    def listdir(self, dname):
        template = 'ls -l %s'
        cmd = template % dname
        os.system(cmd)


    def mklink(self, name, namelink):
        template = 'ln -s %s %s'
        cmd = template % (name, namelink)
        os.system(cmd)


    def remove(self, fname):
        template = 'rm %s'
        cmd = template % fname
        os.system(cmd)

    def ip(self):
        cmd = 'hostname -i'
        fi, fo = os.popen2(cmd)
        fi.close()
        s = fo.read()
        fo.close()
        print s

    def package(self, directory, archivepath):
        if archivepath.endswith('.tar.gz'):
            cmd = 'tar -cvzf %s %s' % (archivepath, directory)
        elif archivepath.endswith('.jar'):
            cmd = 'jar cvMf %s %s' % (archivepath, directory)
        os.system(cmd)

    def extract(self, archivepath):
        if archive.endswith('.tar.gz'):
            cmd = 'tar -xvzf %s' % archivepath
        elif archive.endswith('.jar'):
            cmd = 'jar xvf %s' % archivepath
        os.system(cmd)


########################################################################
# WebUtils
########################################################################

class WebUtils:
    '''
    A container of web related utilities.
    '''
    def download(self, url, localpath):
        template = 'wget --output-document=%s %s'
        cmd = template % (localpath, url)
        os.system(cmd)



########################################################################
# GridUtils
########################################################################
class GridUtils:
    '''
    A container of utilities for interacting with the Grid infrastructure.
    '''
    see = {
        'se': ['se.phy.bg.ac.yu',
               'se001.ipp.acad.bg',
               'se001.imbm.bas.bg',
               'se03.grid.acad.bg',
               'plethon.grid.ucy.ac.cy',
               'ctb32.gridctb.uoa.gr',
               'node004.grid.auth.gr',
               'se.hep.ntua.gr',
               'grid002.ics.forth.gr',
               'se02.marie.hellasgrid.gr',
               'se01.isabella.grnet.gr',
               'se01.marie.hellasgrid.gr',
               'se01.afroditi.hellasgrid.gr',
               'se01.kallisto.hellasgrid.gr',
               'se01.ariagni.hellasgrid.gr',
               'se01.athena.hellasgrid.gr',
               'se01.grid.iasa.gr',
               'cs-grid2.bgu.ac.il',
               'grim-se.iucc.ac.il',
               'testbed002.grid.ici.ro',
               'tbit00.nipne.ro',
               'se-01.cs.tau.ac.il',
               'se.ulakbim.gov.tr',
               'eymir.grid.metu.edu.tr',
               'grid02.erciyes.edu.tr',
               'reyhan.grid.boun.edu.tr',
               'paugrid2.pamukkale.edu.tr',
               'ituse.grid.itu.edu.tr',
               'torik1.ulakbim.gov.tr',
               'wipp-se.weizmann.ac.il'],

        'ce' : ['ce.phy.bg.ac.yu:2119/jobmanager-pbs-see',
                'ce64.phy.bg.ac.yu:2119/jobmanager-pbs-see',
                'ce002.ipp.acad.bg:2119/jobmanager-lcgpbs-see',
                'ce001.imbm.bas.bg:2119/jobmanager-lcgpbs-see',
                'ce02.grid.acad.bg:2119/jobmanager-pbs-see',
                'ce101.grid.ucy.ac.cy:2119/jobmanager-lcgpbs-see',
                'ce301.intercol.edu:2119/jobmanager-lcgpbs-see',
                'ctb31.gridctb.uoa.gr:2119/blah-pbs-see',
                'node001.grid.auth.gr:2119/jobmanager-pbs-see',
                'ce.hep.ntua.gr:2119/jobmanager-lcgpbs-see',
                'grid001.ics.forth.gr:2119/jobmanager-lcgpbs-see',
                'ce02.marie.hellasgrid.gr:2119/jobmanager-pbs-see',
                'ce01.isabella.grnet.gr:2119/jobmanager-pbs-see',
                'ce01.marie.hellasgrid.gr:2119/jobmanager-pbs-see',
                'glite-ce01.marie.hellasgrid.gr:2119/blah-pbs-see',
                'ce01.afroditi.hellasgrid.gr:2119/jobmanager-pbs-see',
                'ce01.kallisto.hellasgrid.gr:2119/jobmanager-pbs-see',
                'ce01.ariagni.hellasgrid.gr:2119/jobmanager-lcgpbs-see',
                'ce01.athena.hellasgrid.gr:2119/jobmanager-pbs-see',
                'ce01.athena.hellasgrid.gr:2119/jobmanager-pbs-long',
                'ce02.grid.iasa.gr:2119/jobmanager-lcgpbs-see',
                'ce01.grid.iasa.gr:2119/blah-pbs-see',
                'cs-grid0.bgu.ac.il:2119/jobmanager-lcgpbs-see',
                'cs-grid1.bgu.ac.il:2119/blah-pbs-see',
                'grim-ce.iucc.ac.il:2119/jobmanager-lcgpbs-see',
                'testbed001.grid.ici.ro:2119/jobmanager-pbs-see',
                'tbit01.nipne.ro:2119/jobmanager-lcgpbs-see',
                'lcfgng.cs.tau.ac.il:2119/jobmanager-pbs-see',
                'ce.ulakbim.gov.tr:2119/jobmanager-lcgpbs-see',
                'cox01.grid.metu.edu.tr:2119/jobmanager-lcgpbs-see',
                'grid01.erciyes.edu.tr:2119/jobmanager-lcgpbs-see',
                'yildirim.grid.boun.edu.tr:2119/jobmanager-lcgpbs-see',
                'paugrid1.pamukkale.edu.tr:2119/jobmanager-lcgpbs-see',
                'ituce.grid.itu.edu.tr:2119/jobmanager-lcgpbs-see',
                'kalkan1.ulakbim.gov.tr:2119/jobmanager-lcgpbs-see',
                'wipp-ce.weizmann.ac.il:2119/jobmanager-lcgpbs-see']
        }



    def mkdir(self, dname):
        template = 'lfc-mkdir -p %s'
        cmd = template % dname
        os.system(cmd)

    def rmdir(self, dname, force=False):
        if not force:
            template = 'lfc-rm -f %s'
        else:
            template = 'rmdir -rf %s'

        cmd = template % dname
        os.system(cmd)


    def listdir(self, dname):
        template = 'lfc-ls -l %s'
        cmd = template % dname
        os.system(cmd)


    def mklink(self, lfn, lfnlink):
        template = 'lfc-ln -s %s %s'
        cmd = template % (lfn, lfnlink)
        os.system(cmd)


    def upload(self, localpath, lfn, se='se01.isabella.grnet.gr', vo='see'):
        template = 'lcg-cr --vo %s -d %s -l %s file://%s'
        cmd = template % (vo, se, lfn, localpath)
        os.system(cmd)


    def download(self, lfn, localpath, vo='see'):
        template = 'lcg-cp --vo %s  %s file://%s'
        cmd = template % (vo, lfn, localpath)
        os.system(cmd)

    def replicate(self, lfn, selist=['se01.isabella.grnet.gr'], vo='see'):
        template = 'lcg-rep --vo %s -d %s %s'
        for se in selist:
            cmd = template % (vo, se, lfn)
            os.system(cmd)

    def remove(self, lfn, selist=['se01.isabella.grnet.gr'], vo='see'):
        template = 'lcg-del --vo %s -s %s %s'
        for se in selist:
            cmd = template % (vo, se, lfn)
            os.system(cmd)











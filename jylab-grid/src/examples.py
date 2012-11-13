import time

def testmtj():
    from no.uib.cipr.matrix import Matrices, DenseMatrix, EVD, SVD
    t = time.time()
    mat = Matrices.random(500,500)
    x = EVD(500)
    x.factor(mat)

    #y = x.getRealEigenvalues()
    #print y
    print time.time()-t

# direct linear solver
def directlinearsolve():
    from no.uib.cipr.matrix import Matrices, DenseMatrix, DenseVector
    from cern.colt.matrix import DoubleMatrix2D, DoubleFactory2D
    from cern.colt.matrix.linalg import Algebra

    sz = 1000
    # MTJ
    x = DenseVector(sz)
    t = time.time()
    mat = Matrices.random(sz,sz)
    b = Matrices.random(sz)
    mat.solve(b,x)
    print "Mtj :" + str(time.time() - t)

    # Colt
    alg = Algebra()
    f = DoubleFactory2D.dense
    t = time.time()
    A = f.random(sz,sz)
    b = f.random(sz,1)
    x = alg.solve(A,b)
    print "Colt :" + str(time.time() - t)


# direct linear solver
def threaded_blas():
    from cern.colt.matrix.linalg import SmpBlas, SeqBlas
    from cern.colt.matrix import DoubleMatrix2D, DoubleFactory2D
    SmpBlas.allocateBlas(2, SeqBlas.seqBlas)

    sz = 1000

    f = DoubleFactory2D.dense
    A = f.random(sz,sz)
    B = f.random(sz,sz)
    C = f.random(sz,sz)


    t = time.time()
    SeqBlas.seqBlas.dgemm(0,0,1,A,B,1,C)
    print "Sequential :" + str(time.time() - t)

    t = time.time()
    SmpBlas.smpBlas.dgemm(0,0,1,A,B,1,C)
    print "Parallel :" + str(time.time() - t)



def testcolt():
    from cern.colt.matrix.linalg import EigenvalueDecomposition
    from cern.colt.matrix import DoubleFactory2D
    #from cern.colt.matrix.impl import DoubleMartix2D
    t = time.time()
    f = DoubleFactory2D.dense
    mat = f.random(500,500)
    x = EigenvalueDecomposition(mat)
    print time.time()-t


def testvisad():
    from visad import RealType, Real, FunctionType, FlatField, RealTuple
    from visad.java3d import DisplayImplJ3D, DirectManipulationRendererJ3D
    from visad.java2d import DisplayImplJ2D, DirectManipulationRendererJ2D
    import subs

    # make Types for our data
    ir_radiance = RealType("ir_raidance")
    count = RealType("count")
    ir_histogram = FunctionType(ir_radiance, count)
    vis_radiance = RealType("vis_radiance")

    # make up the data values...
    histogram = FlatField.makeField(ir_histogram, 64, 0)
    direct = Real(ir_radiance, 2.0)
    direct_tuple = RealTuple( (Real(count, 1.0), Real(ir_radiance,2.0), Real(vis_radiance, 1.0)) )

    # create the scalar mappings for display 0
    maps0=subs.makeMaps(vis_radiance,"z", ir_radiance,"x",
                                        count, "y", count,"green")
    # make display 0 a 3-D display
    dpys0 = subs.makeDisplay3D(maps0)
    subs.setPointSize(dpys0, 5.0)

    # add the data to display 0 and keep the references
    ref_hist = subs.addData("histo", histogram, dpys0,
                    renderer=DirectManipulationRendererJ3D())
    ref_dir = subs.addData("dir", direct, dpys0,
                    renderer=DirectManipulationRendererJ3D())
    ref_dir_tuple = subs.addData("dir_tup", direct_tuple, dpys0,
                    renderer=DirectManipulationRendererJ3D())

    # create the scalar mappings for display 1
    maps1 = subs.makeMaps(ir_radiance,"x", count,"y", count,"green")

    # make display 1 a 2-D display
    dpys1 = subs.makeDisplay2D(maps1)
    subs.setPointSize(dpys1, 5.0)

    # add the data to this display, but use the references from
    # the previous one so the direct manipulations interact with both
    subs.addData("histo", histogram, dpys1,
            renderer=DirectManipulationRendererJ2D(), ref=ref_hist)
    subs.addData("dir", direct, dpys1,
            renderer=DirectManipulationRendererJ2D(), ref=ref_dir)
    subs.addData("dir_tup", direct_tuple, dpys1,
            renderer=DirectManipulationRendererJ2D(), ref=ref_dir_tuple)

    # function to clean up when display window is closed
    def cleanup(event):
        dpys0.destroy()
        dpys1.destroy()
        frame.dispose()

    # create window for display and add the dspy displays to it
    from javax.swing import JPanel, JFrame
    from java.awt import GridLayout
    panel1 = JPanel()
    panel1.add(dpys0.getComponent())
    panel2 = JPanel()
    panel2.add(dpys1.getComponent())

    frame = JFrame("Test35", windowClosing=cleanup)
    pane = frame.getContentPane()
    pane.setLayout(GridLayout(1,2))
    pane.add(panel1)
    pane.add(panel2)
    frame.setSize(600,300)
    frame.setVisible(1)

if __name__ == '__main__':
    testmtj()


def plotblas():
    from visad.python.JPythonMethods import plot
    from hep.aida.ref import Histogram1D
    from math import sin,pi

    (xstart, xend, xnum) = (0.0, 2*pi, 100)
    xstep = (xend - xstart) /(xnum -1)
    xdata = map(lambda x:xstep*x, range(0,xnum))
    ydata = map(sin,xdata)
    plot(ydata)


def natblascheck():
    from no.uib.cipr.matrix.nni import BLAS
    from jarray import array

    x = array([1.1,2.2,3.3,4.4],'d')
    y = array([1.1,2.2,3.3,4.4],'d')

    n = len(x)

    dot = BLAS.dot(n,x,1,y,1)

    print dot


def natlapackcheck():
    from no.uib.cipr.matrix.nni import LAPACK;
    from jarray import array

    iseed = array([1998,1999,2000,2001],'i')
    x = array([0,0,0,0,0,0,0,0,0,0],'d')
    n = array([len(x)],'i')

    LAPACK.laruv(iseed, n, x);

    print "Answer:"
    for i in x:
        print i


class Benchmarks:

    def NNIGEMM(self):

        import time
        tot = time.time()

        pin = []

        for i in range(1,51):
            pin.append(i*10)

        for i in pin:
            print i
            self.gemm(i)

        print "total" +  str((time.time() - tot))

    def gemm(self, n):

        import time
        #from no.uib.cipr.matrix.nni import BLAS,LAPACK;
        from jarray import array, zeros
        from java.lang import Math, System
        import no.uib.cipr.matrix as matrix

        if n < 100:
            r = 100
        else:
            r = 10

        t = time.time()

        #A = zeros(n*n,'d')
        #B = zeros(n*n,'d')
        #C = zeros(n*n,'d')

        pin = range(n*n)

        #for i in pin:
        #    A[i] = Math.random();
        #    B[i] = Math.random();
        #    C[i] = Math.random();

        A = matrix.Matrices.random(n,n);
        B = matrix.Matrices.random(n,n);
        C = matrix.Matrices.random(n,n);


        alpha = Math.random()
        beta = Math.random()

        print "Random numbers time: "+str((time.time() - t))

        for i in range(10):
            #BLAS.gemm(BLAS.ColMajor, BLAS.NoTrans, BLAS.NoTrans, n, n, n, alpha, A, n, B, n, beta, C, n)
            A.multAdd(alpha,B, C)
            C.scale(beta);
            #E=D.mult(C, C.copy())

        t = time.time()

        for i in range(r):
            #BLAS.gemm(BLAS.ColMajor, BLAS.NoTrans, BLAS.NoTrans, n, n, n, alpha, A, n, B, n, beta, C, n)
            #D=A.mult(B, B.copy()); E=D.mult(C, C.copy());
            A.multAdd(alpha,B, C)
            C.scale(beta);

        s = (time.time() - t)
        print s
        f = 2 * (n + 1) * n * n
        mfs = (f / (s * 1000000.0)) * r

        print str(mfs)



def jfigtest():
    from jfig.utils import Array       # several array utilities
    from jfig.utils import MHG         # the plot functions
    m = MHG()                          # initalize the plot library,
                                       # reference via variab
    N = 200
    x = Array.linspace(0,10,N)         # 200 values (0.0 .. 10.0)
    y = Array.cos(x)                   # vector with cos(x)
    z = Array.random(N)                # 200 random values, range [0,1]
    z = Array.add( z, -0.3 )           # shift to range [-0.3, 0.7]

    m.figure();                        # new plot window
    m.gca().autoscaleTight( x, y )     # default scaling
    m.plot( x, y, '-b' )               # solid-blue-line plot
    m.plot( x, z, '.r' )               # red-dot markers
    m.xlabel( 'x' )                    # x-axis label
    m.ylabel( 'sin(x), random data' )  # y-axis label
    m.plotCartesianAxes()              # axes
    m.plotGrid()                       # grid
    m.doZoomFit()

    from java.io import File
    m.save( File( 'plot1.fig' ))       # save figure

def benchfig():

    from jfig.utils import Array       # several array utilities
    from jfig.utils import MHG         # the plot functions
    from jfig.utils import SetupManager         # the plot functions
    from jarray import array

    m = MHG()                          # initalize the plot library,

    resfile = file("/home/kostas/jylabtester/mtjbench","r")
    res = resfile.readlines()

    for i in range(len(res)):
        res[i] = float(res[i].split("\n")[0])

    mtjres = array(res,'d')

    resfile = file("/home/kostas/jylabtester/javabench","r")
    res = resfile.readlines()

    for i in range(len(res)):
        res[i] = float(res[i].split("\n")[0])

    javares = array(res,'d')

    resfile = file("/home/kostas/jylabtester/jlapackbench","r")
    res = resfile.readlines()

    for i in range(len(res)):
        res[i] = float(res[i].split("\n")[0])

    jlres = array(res,'d')

    resfile = file("/home/kostas/jylabtester/nativebench","r")
    res = resfile.readlines()

    for i in range(len(res)):
        res[i] = float(res[i].split("\n")[0])

    natres = array(res,'d')

    resfile = file("/home/kostas/jylabtester/matlabbench","r")
    res = resfile.readlines()

    for i in range(len(res)):
        res[i] = float(res[i].split("\n")[0])

    matres = array(res,'d')

    x = []
    for i in range(1,51):
        x.append(i*10)

    x = array(x,'d')

    SetupManager.getInteger( "jfig.utils.MHG.defaultWindowWidth", 1000 )
    SetupManager.getInteger( "jfig.utils.MHG.defaultWindowHeight", 500 )

    m.figure();                        # new plot window
    m.gca().autoscaleTight( x, natres )     # default scaling
    m.plot( x, natres, '-b' )               # solid-blue-line plot
    m.plot( x, mtjres, '-.r' )               # solid-blue-line plot
    m.plot( x, matres, '-g' )               # solid-blue-line plot
    m.plot( x, javares, '-k' )               # solid-blue-line plot
    m.plot( x, jlres, '-m' )               # solid-blue-line plot

    m.text( 30 , 4150, 'Εκτέλεση του Πολ/σμού C = a*A*B + b*C ', "color=black fontsize=18" )
    m.xlabel( 'Μέγεθος Πίνακα' )                    # x-axis label
    m.ylabel( 'MegaFlops' )  # y-axis label
    m.text( 510, 3700, "--- Jylab JNI Blas", "color=red fontsize=18" );
    m.text( 510, 3400, "--- Blas", "color=blue fontsize=18" );
    m.text( 510, 3100, "--- Matlab", "color=green fontsize=18" );
    m.text( 510, 2800, "--- Pure Java", "color=black fontsize=18" );
    m.text( 510, 2500, "--- JLapack", "color=magenta fontsize=18" );
    m.plotCartesianAxes()              # axes
    m.plotGrid()                       # grid
    #m.doZoomFit()

    from java.io import File
    m.save( File( '/home/kostas/jylabtester/plot3.fig' ))       # save figure


def testjtidy(url = None):
    from java.io import IOException, BufferedInputStream, FileOutputStream, PrintWriter, FileWriter,ByteArrayOutputStream
    from java.net import URL
    from org.w3c.tidy import Tidy

    import re, os
    #This program shows how HTML could be tidied directly from * a URL stream,
    #and running on separate threads. Note the use * of the 'parse' method to
    #parse from an InputStream, and send * the pretty-printed result to an OutputStream.
    # * In this example thread th1 outputs XML, and thread th2 outputs * HTML. This shows
    # that properties are per instance of Tidy. */

    def crawl(site, trm , depth, linksfile):
        from java.net import URL
        from org.w3c.tidy import Tidy
        pattern = re.compile('href="/wiki/(.*?)"')
        f = open(linksfile, 'a+')
        #try:
        if depth < MAX_DEPTH:
            print 'crawling [%s]...' % trm,
            print >> f, '[%s]' % trm

            td = Tidy()
            td.setXmlOut(1)

            u = URL(site + trm)

            input = BufferedInputStream(u.openStream())
            output = ByteArrayOutputStream()
            #tidy.setInputEncoding("UTF8")
            #tidy.setOutputEncoding("UTF8")

            td.parse(input, output)
            content = output.toString()
            hits = pattern.findall(content)

            for hit in hits:
                if hit.find(":") == -1:
                    print >> f, hit
            print 'done.'
            print >> f, ''
            for hit in hits:
                if hit.find(":") == -1:
                    crawl(site, hit, depth + 1, linksfile)
        #except:
        #    print "wrong"
        f.close()

    MAX_DEPTH=3
    base = 'http://en.wikipedia.org/wiki/'
    term = 'Web_mining'
    linksfile = '/home/kostas/jylabtester/links.txt'

    if os.path.isfile(linksfile):
        os.remove(linksfile)
    crawl(base,term, 0, linksfile)

def testproximus():
    from proximus import *
    from proximus.linux import NLib

    datapath = '/home/kostas/proximus/sample/A.mtx'
    nlib = NLib("libbnd.so")
    lib = nlib.getLib()

    poolsize = 2**28
    tempsize = 2**24
    bytes = initMemoryPool(poolsize, tempsize, lib)
    matrix = readMatrix(datapath, lib)

    print bytes
    print matrix.m, matrix.n, matrix.nz
    vector = initVector(100, lib)

    displayVector(vector, lib)
    print vector.n
    print vector.max

def grejob():
    from grid import *
    j = Job()
    j.inputsandbox = ["/home/kostas/test.py"]
    j.arguments = "test.py"
    j.inputdata = ["lfn:/grid/see/jylab/cnr-2000.graph","lfn:/grid/see/jylab/cnr-2000.properties"]
    j.ce = ces(17)
    j.submit()

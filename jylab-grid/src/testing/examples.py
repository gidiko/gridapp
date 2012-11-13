from cern.colt.function import Double9Function
from cern.colt.matrix.impl import DenseDoubleMatrix2D
from graph import *
from visad.util import Util

class JacobiStencil (Double9Function):
    def apply( self , a00, a01, a02, a10, a11, a12, a20, a21, a22):
        return 0.25 * (a01 + a10 + a12 + a21)

jacobiStencil = JacobiStencil()

(rows, columns) = (50,50)

matrix = DenseDoubleMatrix2D(rows, columns)
newmatrix = DenseDoubleMatrix2D(rows, columns)
matrix.assign (0.0)
matrix.viewRow(0).assign (1.0)
matrix.viewRow(rows - 1).assign(1.0)
matrix.viewColumn(0).assign (1.0)
matrix.viewColumn(columns - 1).assign (1.0)

for i in range(200):
    matrix.zAssign8Neighbors(newmatrix, jacobiStencil )
    matrix = newmatrix

data = list(map( list , matrix.toArray()))
display = image(data, width=200, height=200)
Util.captureDisplay ( display , '/home/kostas/stencil.jpg' , 1)
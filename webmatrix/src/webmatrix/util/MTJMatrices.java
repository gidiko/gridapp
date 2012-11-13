package webmatrix.util;

import no.uib.cipr.matrix.*;
import no.uib.cipr.matrix.sparse.*;
import java.io.*;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import webmatrix.*;


public class MTJMatrices {
    
    public static Matrix setMatrixColumn(Matrix A, int j, Vector v) {
	int n = A.numRows();
	for(int i = 0; i < n; i++) {
	    A.set(i, j, v.get(i));
	}
	return A;
    }
    
    public static Vector getMatrixColumn(Matrix A, int j) {
	int n = A.numRows();
	Vector x = new DenseVector(n);
	for (int i = 0;  i< n; i++) {
	    x.set(i, A.get(i, j));
	}
	return x;
    }
    
    /**
     * Loads sparse matrix from binary file assuming webgraph format.
     * Sparse matrix is a no.uib.cipr.matrix.sparse.CompRowMatrix instance i.e.
     * a sparse matrix in CRS format.
     * 
     * @param filename  name of file.
     * @return sparse matrix. 
     * @throws IOException if file cannot be read.
     */     
    public static Matrix load(String filename) throws IOException {
	WebGraph graph = WebGraph.load(filename);
	return convert(graph);
    }
    
    /**
     * Converts web graph to sparse matrix.
     * Sparse matrix is a no.uib.cipr.matrix.sparse.CompRowMatrix instance i.e.
     * a sparse matrix in CRS format.
     *
     * @param graph web graph.
     * @return sparse matrix. 
     */         
    public static Matrix convert(WebGraph graph) {
	// get coordinate format
	// row[], column[], entry[]
	int numRows = graph.getSize();
	int numColumns = graph.getN();
	int numEntries = graph.getMultilinks();
        int[] row = graph.computeRowIndices();
        int[] column = graph.computeColumnIndices();
        double[] entry = graph.computeData();
	// allow graph to garbage collector
	graph = null;
	// convert coordinate format to CRS format
	// convert row[], column[] to CRS format
	int[][] nz = coordToCRS(row, column, numRows);
	Matrix matrix = new CompRowMatrix(numRows, numColumns, nz);
	// insert entry[] into sparse matrix
	for (int i = 0; i < numEntries; ++i)
            matrix.set(row[i], column[i], entry[i]);
	return matrix;
    }
    
    /**
     * Returns CRS structure.
     * By CRS structure we mean a 2D array of indices:
     * Each row i of this array contains column indices j 
     * of nonzero elements a_ij in this row.
     *
     * @param row indices of nonzero elements
     * @param column indices of nonzero elements
     * @param numRows number of rows
     * @return CRS structure 
     */       
    public static int[][] coordToCRS(int[] row, int[] column, int numRows) {
	int numEntries = row.length;
	List<Set<Integer>> rnz = new ArrayList<Set<Integer>>(numRows);
        for (int i = 0; i < numRows; ++i)
            rnz.add(new HashSet<Integer>());
	
        for (int i = 0; i < numEntries; ++i)
            rnz.get(row[i]).add(column[i]);
	
	int[][] nz = new int[numRows][];
        for (int i = 0; i < numRows; ++i) {
            nz[i] = new int[rnz.get(i).size()];
            int j = 0;
            for (Integer colind : rnz.get(i))
                nz[i][j++] = colind;
        }
	return nz;
    }
    
    /**
     * Returns CRS structure.
     * By CRS structure we mean a 2D array of indices:
     * Each row i of this array contains column indices j 
     * of nonzero elements a_ij in this row. We assume
     * that the last row of the matrix contains nonzero elements.
     *
     * @param row indices of nonzero elements
     * @param column indices of nonzero elements
     * @return CRS structure 
     */         
    public static int[][] coordToCRS(int[] row, int[] column) {
	int numRows = IntArrays.max(row) + 1;
	return coordToCRS(row, column, numRows);
    }
    
     /**
     * Returns coordinate-format row indices of nonzero elements given a CRS structure.
     * By CRS structure we mean a 2D array of indices:
     * Each row i of this array contains column indices j 
     * of nonzero elements a_ij in this row. 
     * 
     * @param nz CRS structure
     * @return row indices of nonzero elements
     */            
    public static int[] computeRowIndices(int[][] nz) {
	int nnz = 0;
	int m = nz.length;
	for (int i = 0; i < m; ++i) {
	    nnz += nz[i].length;
	}
	
	int[] row = new int[nnz];
	int counter = 0;
	for (int i = 0; i < m; i++) {
	    int n = nz[i].length;
	    for (int j = 0; j < n; j++) {
		row[counter] = i;
		counter++;
	    }
	}
	return row;
    }
    
     /**
     * Returns coordinate-format column indices of nonzero elements given a CRS structure.
     * By CRS structure we mean a 2D array of indices:
     * Each row i of this array contains column indices j 
     * of nonzero elements a_ij in this row. 
     * 
     * @param nz CRS structure
     * @return column indices of nonzero elements
     */       
    public static int[] computeColumnIndices(int[][] nz) {
	int[] column = IntArrays.flatten(nz);
	return column;
    }  
    
    /**
     * Assigns <code>value</code> to all nonzero elements of sparse matrix.
     *
     * @param m sparse matrix to change.
     * @param value value to assign. 
     */			
    
    public static void assign(CompRowMatrix m, double value) {
	double[] data = m.getData();
	DoubleArrays.assign(data, value);
    }
    
    public static DenseVector newUniformVector(int size) {
	double value = 1.0 / size;
	double[] data = DoubleArrays.repeat(value, size);
	return new DenseVector(data);
    }
    
    public static DenseVector newRandomVector(int size) {
	double[] randoms = DoubleArrays.random(size);
	double sum = DoubleArrays.sum(randoms);
	double[] data = DoubleArrays.scale(randoms, 1.0 / sum);
	return new DenseVector(data);
    }
    

    public static Matrix randomPatternMatrix(int numRows, int numColumns, double maxSparsity, long seed) {
	int maxNumEntries = (int)(maxSparsity * (numRows * numColumns));
	int[] row = new int[maxNumEntries];
	int[] column = new int[maxNumEntries];
	Random random = new Random(seed);
	for (int i = 0; i < maxNumEntries; i++) {
	    row[i] = random.nextInt(numRows);
	    column[i] = random.nextInt(numColumns);
	}
	// nz may have less than maxNumEntries elements in total
	int[][] nz = coordToCRS(row, column, numRows);
	Matrix matrix = new CompRowMatrix(numRows, numColumns, nz);
	return matrix;
    }
    
    public static Matrix randomPatternMatrix(int numRows, int numColumns, double maxSparsity) {
	return randomPatternMatrix(numRows, numColumns, maxSparsity, 100L);
    }
    
    
    public static CompRowMatrix random(int numRows, int numColumns, double maxSparsity, long seed) {
	Matrix m = randomPatternMatrix(numRows, numColumns, maxSparsity, seed);
	// not a deep copy
	CompRowMatrix matrix = new CompRowMatrix(m, false);
	// reuse seed
	Random random = new Random(seed);
	double[] data = matrix.getData();
	int numEntries = data.length;
	for(int i = 0; i < numEntries; i++) {
	    data[i] = random.nextDouble();
	}
	return matrix;
    }

    
    
    public static CompRowMatrix random(int numRows, int numColumns, double maxSparsity) {
	return random(numRows, numColumns, maxSparsity, 100L);
    }
    
    // j -> i
    public static CompRowMatrix newRandomAdjacencyMatrix(int numRows, int numColumns, double maxSparsity) {
	Matrix m = randomPatternMatrix(numRows, numColumns, maxSparsity);
	CompRowMatrix matrix = new CompRowMatrix(m, false);
	assign(matrix, 1.0);
	return matrix;
    }
    
    // j -> i
    public static CompRowMatrix newRandomTransitionMatrix(int numRows, int numColumns, double maxSparsity) {
       	Matrix m = randomPatternMatrix(numRows, numColumns, maxSparsity);
	CompRowMatrix matrix = new CompRowMatrix(m, false);
	int[] outlinks = computeOutlinks(matrix);
	double[] invOutlinks = new double[numColumns];
	for (int i = 0; i < numColumns; i++) {
	    try {
		invOutlinks[i] = 1.0 / outlinks[i];
	    } catch (ArithmeticException ae) {
		invOutlinks[i] = 1.0;
	    }
	}
	double[] data = matrix.getData();
	int[] indices = matrix.getColumnIndices();
	int numEntries = data.length;
	for (int i = 0; i < numEntries; i++) {
	    int column = indices[i];
	    data[i] = invOutlinks[column];
	}
	return matrix;
    }
      
    
    public static int[] computeOutlinks(CompRowMatrix matrix) {
	int numEntries = matrix.getData().length;
	int[] outlinks = new int[matrix.numColumns()];
	int[] indices = matrix.getColumnIndices();
	for (int i = 0; i < numEntries; i++) {
	    outlinks[indices[i]]++;
	}
	return outlinks;
    }
    

    // uniqueAncestors, A^t
    public static CompRowMatrix ancestorsToAdjacencyMatrix(int[][] ancestors) {
	int numRows = ancestors.length;
	int numColumns = numRows;
	CompRowMatrix matrix = new CompRowMatrix(numRows, numColumns, ancestors);
	assign(matrix, 1.0);
	return matrix;
    }
    
    public static int[] ancestorsToOutlinks(int[][] ancestors) {
	int size = ancestors.length;
	int[] data = new int[size];
	for(int i = 0; i < size; i++) {
	    int[] sources = ancestors[i];
	    for(int j = 0; j < sources.length; j++) {
		data[sources[j]]++; 
	    }
	} 
	return data;
    }
    
    public static double[] invertOutlinks(int[] outlinks) {
	int size = outlinks.length;
	double[] data = new double[size];
	for (int i = 0; i < size; i++) {
	    try {
		data[i] = 1.0 / outlinks[i];
	    } catch (ArithmeticException ae) {
		data[i] = 1.0;
	    }
	}
	return data;
    }
    
    
    public static CompRowMatrix ancestorsToTransitionMatrix(int[][] ancestors) {
	CompRowMatrix matrix = ancestorsToAdjacencyMatrix(ancestors);
	int[] outlinks = ancestorsToOutlinks(ancestors);
	double[] invOutlinks = invertOutlinks(outlinks);
	double[] data = matrix.getData();
	int[] columnIndices = matrix.getColumnIndices();
	int numEntries = data.length;
	for(int i = 0; i < numEntries; i++) {
	    data[i] = data[i] * invOutlinks[columnIndices[i]];
	}
	return matrix;
    }
    
    
    
    
    public static CompRowMatrix successorsToAdjacencyMatrix(int[][] sucessors) {
	int[][] ancestors = IntArrays.reverse(sucessors);
	return ancestorsToAdjacencyMatrix(ancestors);
    }
    
    public static int[] successorsToOutlinks(int[][] successors) {
	int size = successors.length;
	int[] data = new int[size];
	for(int i = 0; i < size; i++) {
	    data[i] = successors[i].length;
	} 
	return data;
    }
    
    
    
    //P^t
    // public static CompRowMatrix ancestorsToTransitionMatrix(int[][] ancestors) {
    // }
    

    public static int[] computeDanglingPages(Matrix matrix) {
	int numColumns = matrix.numColumns();
	double[] data = DoubleArrays.repeat(1.0, numColumns);
	Vector ones = new DenseVector(data);
	Vector columnSums = matrix.transMult(ones, ones.copy());
	java.util.Vector<Integer> danglingPagesVector = new java.util.Vector<Integer>();
	for (int i = 0; i < numColumns; i++) {
            if (columnSums.get(i) == 0.0) {
                danglingPagesVector.add(i);
            }
        }
	int[] danglingPages = IntArrays.toArray(danglingPagesVector);
	return danglingPages;
    }
    
    
    public static Vector computeDanglingVector(Matrix matrix) {
	int size = matrix.numColumns();
	int[] index = computeDanglingPages(matrix);
	Vector dangling = new SparseVector(size);
	for (int i = 0; i < index.length; i++) {
	    dangling.set(index[i], 1.0);
	}
	return dangling;
    }
    
    public static DenseVector ordering(DenseVector v) {  
	double[] data = v.getData();
	int size = data.length;
	double[] datasorted = new double[size];
	int[] order = new int[size];
	System.arraycopy(data, 0, datasorted, 0, size);
	Arrays.sort(datasorted);
	datasorted = DoubleArrays.reverse(datasorted); // big first
	for(int i = 0; i < size; i++) {
	    int pos = Arrays.binarySearch(datasorted, data[i]);
	    order[i] = pos;
	}
	return new DenseVector(IntArrays.scale(order, 1.0));
    }

}

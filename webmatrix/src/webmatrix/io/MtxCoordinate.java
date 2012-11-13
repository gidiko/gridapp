package webmatrix.io;

import java.io.*;
import webmatrix.*;
import webmatrix.util.*;

public class MtxCoordinate {
	
	public static WebGraph load(String filename) throws IOException {
		MatrixVectorReader r = new MatrixVectorReader(new InputStreamReader(new DataInputStream(new BufferedInputStream(new FileInputStream(filename)))));
		
		
		// Get matrix information. Use the header if present, else use a safe
        // default
        MatrixInfo minfo = null;
        if (r.hasInfo())
            minfo = r.readMatrixInfo();
        else
            minfo = new MatrixInfo(true, MatrixInfo.MatrixField.Real,
                    MatrixInfo.MatrixSymmetry.General);
        MatrixSize msize = r.readMatrixSize(minfo);

        // Resize the matrix to correct size
        int m  = msize.numRows();
        int n  = msize.numColumns();
		int nnz = msize.numEntries();
        // Start reading entries
        int[] icoord = new int[nnz];
		int[] jcoord  = new int[nnz];
        double[] data = new double[nnz];
        r.readCoordinate(icoord, jcoord, data);

        // Shift the indices from 1 based to 0 based
        r.add(-1, icoord);
        r.add(-1, jcoord);
		
		
		////
		// fill up link info, copied from above
		int[] inlinks = new int[m];
		int[][] ancestors = new int[m][];
		int[] outlinks = new int[n];
		double[] units = new double[n];
		
		
		int[] outmarks = new int[n];
		double[][] outdata = new double[n][];
		
		for (int k = 0; k < nnz; k++) {
			outmarks[jcoord[k]]++;
		}
		
		for (int i = 0; i < n ; i++) {
			int num = outmarks[i];
			outdata[i] = new double[num];
		}
		
		int[] counters = new int[n];
		for (int k = 0; k < nnz; k++) {
			int ancestor = jcoord[k];
			outdata[ancestor][counters[ancestor]] = data[k];
			counters[ancestor]++;
		} 
		
		for (int i = 0; i < n; i++) {
			units[i] = DoubleArrays.gcd(outdata[i]);
		}
		
		for (int k = 0; k < nnz; k++) {
			int ancestor = jcoord[k];
			int successor = icoord[k];
			int links = (int)Math.round(data[k] / units[ancestor]);
			inlinks[successor] = inlinks[successor] + links;
			outlinks[ancestor] = outlinks[ancestor] + links;
		}
		
		
		for (int i = 0; i < m; i++) {
			int indegree = inlinks[i];
			ancestors[i] = new int[indegree];
		}
		
		int[] incounters = new int[m];
		for (int k = 0; k < nnz; k++) {
			int ancestor = jcoord[k];
			int successor = icoord[k];
			int links = (int)Math.round(data[k] / units[ancestor]);
			for(int j = 0; j < links; j++) {
				ancestors[successor][incounters[successor]] = ancestor;
				incounters[successor]++;
			}
		}
		return new WebGraph(inlinks, ancestors, outlinks);
	}

		// XXX infinity for jcoord=1, the same must be for dumpBinCoord()
	public static void dump(WebGraph graph, String filename) throws IOException {
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filename)));
		
		int m = graph.getSize();
		int n = graph.getN();
		
		int nnz = graph.getMultilinks();
		
		// fill up coordinate format info...
		int[] icoord = graph.computeRowIndices();
		int[] jcoord = graph.computeColumnIndices();
		double[] data = graph.computeData();
		
		int offset = 1;
		MatrixInfo.MatrixField elementsType = MatrixInfo.MatrixField.valueOf("Real");
		MatrixInfo.MatrixSymmetry symmetryType = MatrixInfo.MatrixSymmetry.valueOf("General");
		MatrixInfo minfo = new MatrixInfo(true, elementsType, symmetryType);
		MatrixSize msize = new MatrixSize(m, n, nnz);
		MatrixVectorWriter mvw = new MatrixVectorWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename))));
		mvw.printMatrixInfo(minfo);
		String[] comments = {" "}; 
		mvw.printComments(comments);
		mvw.printMatrixSize(msize, minfo);
		mvw.printCoordinate(icoord, jcoord, data, offset);
		mvw.flush();
		mvw.close();		
	}
	
	
}

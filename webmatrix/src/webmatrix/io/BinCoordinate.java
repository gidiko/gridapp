package webmatrix.io;

import java.io.*;
import webmatrix.*;
import webmatrix.util.*;


public class BinCoordinate {
	/**
	 * Loads webgraph from binary file assuming coordinate format.
	 *
	 * @param filename  name of file.
	 * @throws IOException if file cannot be read.
	 */     
	public static WebGraph load(String filename) throws IOException {
		DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(filename)));
		int m = dis.readInt();
		int n = dis.readInt();
		int nnz = dis.readInt();
		int[] icoord = new int[nnz];
		int[] jcoord = new int[nnz];
		double[] data = new double[nnz];
		for (int k = 0; k < nnz; k++) {
			icoord[k] = dis.readInt();
		}
		for (int k = 0; k < nnz; k++) {
			jcoord[k] = dis.readInt();
		}
		for (int k = 0; k < nnz; k++) {
			data[k] = dis.readDouble();
		}				
		
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
	
	
	/**
	 * Stores web graph to binary file using coordinate format. 
	 * The binary file successively contains:
	 * <p>
	 * <table border="1">
	 * <tr>
	 * <td><b>datatype</b></td>
	 * <td><b>name</b></td>
	 * <td><b>description</b></td>
	 * </tr>
	 * <tr>
	 * <td><code>int</code></td>
	 * <td><code>m</code></td>
	 * <td>number of rows</td>
	 * </tr>
	 * <tr>
	 * <td><code>int</code></td>
	 * <td><code>n</code></td>
	 * <td>number of columns</td>
	 * </tr>
	 * <tr>
	 * <td><code>int</code></td>
	 * <td><code>nnz</code></td>
	 * <td>number of nonzero elements</td>
	 * </tr>
	 * </tr>
	 * <tr>
	 * <td><code>int[nnz]</code></td>
	 * <td><code>icoord</code></td>
	 * <td>vector of i-coordinates</td>
	 * </tr>
	 * <tr>
	 * <td><code>int[nnz]</code></td>
	 * <td><code>jcoord</code></td>
	 * <td>vector of j-coordinates</td>
	 * </tr>
	 * <tr>
	 * <td><code>double[nnz]</code></td>
	 * <td><code>data</code></td>
	 * <td>vector of values</td>
	 * </tr>
	 * </table>
	 * </p>
	 * <p>
	 * Total number of bytes: <code> 12 + 16 * nnz </code>
	 * </p>
	 * @param filename  name of file.
	 * @throws IOException if file cannot be written.
	 */    	
	public void dump(WebGraph graph, String filename) throws IOException {
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filename)));
		int m = graph.getSize();
		int n = graph.getN();
		
		int nnz = graph.getMultilinks();
				
		dos.writeInt(m);
		dos.writeInt(n);
		dos.writeInt(nnz);
		// fill up coordinate format info...
		int[] icoord = graph.computeRowIndices();
		int[] jcoord = graph.computeColumnIndices();
		double[] data = graph.computeData();
		
		// ...and dump it to file
		for (int k = 0; k < nnz; k++) {
			dos.writeInt(icoord[k]);
		}
		for (int k = 0; k < nnz; k++) {
			dos.writeInt(jcoord[k]);
		}
		for (int k = 0; k < nnz; k++) {
			dos.writeDouble(data[k]);
		}		
		dos.close();
	}
	
	


}

package webmatrix.io;

import java.io.*;
import webmatrix.util.*;
import webmatrix.*;

public class BinArray {
	
	
	/**
	 * Loads ranklet data from binary file. 
	 * It is assumed that returned ranklet starts at 0.
	 *
	 * @param filename  name of file.
	 * @return ranklet.
	 * @throws IOException if file cannot be read.
	 */    	   	
	public static Ranklet load(String filename) throws IOException {
		double[][] dataRead = DoubleArrays.load(filename);
		int m = dataRead.length;
		double[] data = null;
		if (m == 1) {
			data = DoubleArrays.packByRows(dataRead);
		} else {
			data = DoubleArrays.packByColumns(dataRead);
		}
		return new Ranklet(data);
	}
	
   	
	/**
	 * Stores ranklet data to binary file. 
	 * Start position fro the ranklet is not saved.
	 *
	 * @param rank ranklet to store.
	 * @param filename  name of file.
	 * @param row if true, writes data array as a row vector (m=1).
	 * @throws IOException if file cannot be written.
	 */    	   	
	public static void dump(Ranklet rank, String filename, boolean row) throws IOException {
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filename)));
		int num = rank.getSize();
		double[] data = rank.getData();
		int m;
		int n;
		if (row) {
			m = 1;
			n = num;
		} else {
			m = num;
			n = 1;
		}		
		dos.writeInt(m);
		dos.writeInt(n);
		for (int i = 0; i < num; i++) {
			dos.writeDouble(data[i]);
		} 
		dos.close();
	}

	
	public static void dump(Ranklet rank, String filename) throws IOException {
		dump(rank, filename, true);
	}
}

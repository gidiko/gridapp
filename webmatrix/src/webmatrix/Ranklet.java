package webmatrix;

import java.io.*;
import webmatrix.util.*;

public class Ranklet implements Serializable {
    /**
	 *
	 */
	private static final long serialVersionUID = 1L;
	// To be serialized
	int start;
    int end;
	double[] data;
	// Not to be serialized
    int size;

	public Ranklet(double[] data) {
		this(0, data);
	}


	public Ranklet(int start, double[] data) {
		this(start, start + data.length, data);
    }


    public Ranklet(int start, int end, double[] data) {
		this(start, end, data, true);
    }


    public Ranklet(int start, int end, double[] data, boolean deep) {
		this.start = start;
		this.end = end;
		size = end - start;
		if (deep) {
			this.data = new double[size];
			System.arraycopy(data, 0, this.data, 0, size);
		} else {
			this.data = data;
		}
    }


    public Ranklet[] split(int m) {
		int[][] limits = IntArrays.partLimits(size, m, start);
		return split(limits);
    }


    public Ranklet[] split(int[] sizes) {
		int m = sizes.length;
		int[][] limits = new int[m][2];
		int cursor = start;
		for (int i = 0; i < m; i++) {
			limits[i][0] = cursor;
			limits[i][1] = cursor + sizes[i];
			cursor = cursor + sizes[i];
		}
		return split(limits);
    }


    public Ranklet[] split(int[][] limits) {
		int m = limits.length;
		Ranklet[] ranklets = new Ranklet[m];
		for (int i = 0; i < m; i++) {
			int start = limits[i][0];
			int end = limits[i][1];
			int size = end - start;
			double[] partData = new double[size];
			System.arraycopy(data, start, partData, 0, size);
			ranklets[i] = new Ranklet(start, end, partData);
		}
		return ranklets;
    }


    public static Ranklet pack(Ranklet[] ranklets) {
		int parts = ranklets.length;
		int start = ranklets[0].start;
		int end = ranklets[parts - 1].end;
		int size = end - start;
		double[] data = new double[size];

		int cursor = 0;
		for (int i = 0; i < parts; i++) {
			Ranklet ranklet = ranklets[i];
			int partSize = ranklet.size;
			double[] partData = ranklet.data;
			System.arraycopy(partData, 0, data, cursor, partSize);
			cursor = cursor + partSize;
		}
		return new Ranklet(start, data);
    }

    public void dump(String filename) throws IOException {
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filename)));
		dos.writeInt(start);
		dos.writeInt(end);
		for (int i = 0; i < size; i++) {
			dos.writeDouble(data[i]);
		}
		dos.close();
    }


    public static Ranklet load(String filename) throws IOException {
		DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(filename)));
		int start = dis.readInt();
		int end = dis.readInt();
		int size = end - start;
		double[] data = new double[size];
		for (int i = 0; i < size; i++) {
			data[i] = dis.readDouble();
		}
		dis.close();
		return new Ranklet(start, end, data, false);
    }



	//////////////////////////////////////////////////////////////////////
	// getters
	//////////////////////////////////////////////////////////////////////
	/**
	  * Returns page number corresponding to the first element in rank vector.
	  * Note that ranks for <code>size</code> consecutive pages are included in
	  * rank vector.
	  *
	  * @return first page number.
	  */
	 public int getStart() {
		return start;
	}

	/**
	 * Returns first page number represented by the first element of next rank
	 * vector (last page number of this vector minus one)
	 *
	 * @return last page number of this vector minus one.
	 */
	 public int getEnd() {
		return end;
	}

	/**
	 * Returns data in rank vector.
	 *
	 * @return data in rank vector.
	 */
	public double[] getData() {
		return data;
	}

	/**
	 * Returns size of rank vector (number of its elements).
	 *
	 * @return size of rank vector (number of its elements).
	 */
	public int getSize() {
		return size;
	}


	/**
	 * Returns the sum of elements in the ranklet.
	 *
	 * @return sum of elements.
	 */
	public double sum() {
		return DoubleArrays.sum(getData());
	}

	public Ranklet copy() {
		return new Ranklet(getStart(), getData());
	}

	/**
	 * Prints info for rank vector.
	 */
	public void info() {
		System.out.print("start = ");
		System.out.println(start);
		System.out.print("end = ");
		System.out.println(end);
		System.out.print("sum = ");
		System.out.println(sum());
	}

}

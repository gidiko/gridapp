package webmatrix;

import java.io.*;
import java.util.Arrays;
import java.lang.System;
import webmatrix.util.*;


public class PageRank extends Ranklet {
	
    public PageRank(Ranklet ranklet) {
		this(ranklet.data);
		normalize();
    }
    
    public PageRank(double[] data) {
		super(data);
		normalize();
    }
        
    public void normalize() {
		data = DoubleArrays.normalize(data);
    }
	
	/**
	 * Compresses pagerank to a new size.
	 *
	 * @param m size of the compressed pagerank.
	 * This is just the number of the relevant nodes (pages).
	 * @return compressed pagerank vector.
	 */         	
	public PageRank compress(int m) {
		// start == 0, size >= m
		int[][] limits = IntArrays.partLimits(size, m, start);
		return compress(limits);
    }
    
  	/**
	 * Compresses pagerank into successive parts with given sizes. 
	 *
	 * @param sizes sizes of parts to compress.
	 * @return compressed pagerank vector.
	 */         
    public  PageRank compress(int[] sizes) {
		// start == 0
		int m = sizes.length;
		int[][] limits = new int[m][2];
		int cursor = start;
		for (int i = 0; i < m; i++) {
			limits[i][0] = cursor;
			limits[i][1] = cursor + sizes[i];
			cursor = cursor + sizes[i]; 
		}
		return compress(limits);
    }
    
	/**
	 * Compresses pagerank into parts defined by successive page ranges. 
	 * Limits for these ranges are given as <code>[limits[][0],
	 * limits[][1])</code>. Pages within such a range are assumed to have
	 * collapsed to a single page in the compressed graph. 
	 * 
	 * @param limits limits for the ranges of webgraph pages to compress.
	 * @return compressed pagerank vector.
	 */       
    public PageRank compress(int limits[][]) {
		int m = limits.length;
		double[] dataCompressed = new double[m];
		for (int i = 0; i < m; i++) {
			int start = limits[i][0];
			int end = limits[i][1];
			double sum = 0.0;
			for (int j = start; j < end; j++) {
				sum = sum + data[j];
			}
			dataCompressed[i] = sum;
		}
		return new PageRank(dataCompressed);
	}
	
	
	public PageRank expand(int n) {
		// start == 0
		int[][] limits = IntArrays.partLimits(n, size, start);
		return expand(limits);
    }
	
	
    public PageRank expand(int[] sizes) {
		// m == size, start == 0
		int m = sizes.length;
		int[][] limits = new int[m][2];
		int cursor = start;
		for (int i = 0; i < m; i++) {
			limits[i][0] = cursor;
			limits[i][1] = cursor + sizes[i];
			cursor = cursor + sizes[i]; 
		}
		return expand(limits);
    }
	
	
    public PageRank expand(int limits[][]) {
		// m == size, start = 0
		int m = limits.length;
		int n = limits[m - 1][1];
		double[] expandedData = new double[n];
		for (int i = 0; i < m; i++) {
			int start = limits[i][0];
			int end = limits[i][1];
			int size = end - start;
			for (int j = start; j < end; j++){ 
				expandedData[j] = data[i] / size;
			}
		}
		return new PageRank(expandedData);
	}
	
	/**
	 * Permutes entries in a pagerank vector according to given permutation
	 * vector (<code>map</code>).
	 * entry[i] <- entry[map[i]].  
	 *
	 * @param map permutation vector
	 * @return pagerank with permutation applied
	*/
	public PageRank permute(int[] map) {
		return new PageRank(DoubleArrays.permute(data, map));
	}


	
	/**
	 * Returns ranking order of pages, meaning their relative order imposed by
	 * their pagerank values.
	 * ranks[i] contains the integer rank of pagerank[i]. It
	 * follows that maximum pagerank value is found for i with ranks[i] = 0
	 * 
	 * @return ranking order of pages.
	 */
    public int[] ranking() {
		return DoubleArrays.ranking(data);
	}
	
	/**
	 * Returns a permutation which when applied to pagerank will sort it in
	 * descending order. permutation[i] contains the position of the i-th
	 * largest entry in pagerank. It follows that permutation[0] is the index to
	 * largest pagerank value. 
	 *  
	 * @return descending order pagerank permutation.
	 */	
	public int[] rankingPermutation() {
		return DoubleArrays.rankingPermutation(data);
	}

	/**
	 * Zeros pagerank values indexed by <code>index</code> and distributes their
	 * values uniformly to the rest. Sum is preserved.
	 *
	 * @param data data array.
	 * @param index index array of elements to zero.
	 * @return pagerank with zerod positions.
	 */
	public PageRank neutral(int[] index) {
		return new PageRank(DoubleArrays.neutral(data, index));
	}
		
}

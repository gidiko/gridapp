package webmatrix;

import webmatrix.util.*;

public class Ranks {
    /**
     * Returns a random pagerank vector for specified number of pages. 
     * 
     * @param n number of pages.
     */
    public static PageRank newRandomRank(int n) {
	double[] data = DoubleArrays.random(n);
	return new PageRank(data);
    }
	
    /**
     * Returns a uniform pagerank vector for specified number of pages. 
     * 
     * @param n number of pages.
     */	
    public static PageRank newUniformRank(int n) {
	double[] data = new double[n];
	double value = 1.0;
	for (int i = 0; i < n; i++) {
	    data[i] = value;
	}
	// Normalization will happen!
	return new PageRank(data);
    }
	

    /**
     * Returns norm1 of the difference of two ranklets. 
     * Norm1 is the sum of the absolute values.
     * 
     * @param x a ranklet.
     * @param y the other ranklet.
     * @return norm2.
     */				
    public static double diffNorm1(Ranklet x, Ranklet y) {
	return DoubleArrays.diffNorm1(x.getData(), y.getData());
    }
    
    
    /**
     * Returns norm2 of the difference of two ranklets. 
     * Norm2 is also known as the Euclidean distance: the square root of the sum
     * of squares.
     * 
     * @param x a ranklet.
     * @param y the other ranklet.
     * @return norm2.
     */				
    public static double diffNorm2(Ranklet x, Ranklet y) {
	return DoubleArrays.diffNorm2(x.getData(), y.getData());
    }
    

    
	
}
		
	
   

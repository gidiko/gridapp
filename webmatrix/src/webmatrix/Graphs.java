package webmatrix;

import java.util.Random;
import java.util.Vector;
import java.util.Enumeration;
import java.io.*;

import webmatrix.util.*;

/**
 * Factory methods for the construction of various types of web graphs.
 *
 */
public class Graphs implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Returns a Barabasi-Albert web graph with specified numbers of pages and outlinks per
	 * page.
	 *
	 * @param n number of pages
	 * @param outdegree number of outlinks per page
	 * @return Barabasi-Albert web graph
	 */			
	public static WebGraph newBarabasiAlbertGraph(int n, int outdegree) {
		return newBarabasiAlbertGraph(n, outdegree, 100L);
	}
	
	
	/**
	 * Returns a Barabasi-Albert web graph with specified numbers of pages, outlinks per
	 * page and random number generator seed. 
	 * This graph is created using a preferential attachment algorithm inspired
	 * by the construction described in:
	 * <p>
	 * <code>
	 * <p>
	 * ARTICLE{barabasi-1999-emergence,
	 * </p>
	 * <p>
	 * TITLE = {Emergence of scaling in random networks},
	 * </p>
	 * <p>
	 * AUTHOR = {A. L. Barabasi and R. Albert},
	 * </p>
	 * <p>
	 * JOURNAL = {SCIENCE},
	 * </p>
	 * <p>
	 * VOLUME = {286},
	 * </p>
	 * <p>
	 * NUMBER = {5439},
 	 * </p>
	 * <p>
	 * PAGES = {509 -- 512},
 	 * </p>
	 * <p>
	 * MONTH = {OCT},
 	 * </p>
	 * <p>
	 * YEAR = {1999},
 	 * </p>
	 * <p>
	 * }
 	 * </p>
	 * </code>
	 * </p>
	 *
	 * @param n number of pages
	 * @param outdegree number of outlinks per page
	 * @param seed seed for random number generator
	 * @return Barabasi-Albert web graph
	 */			

public static WebGraph newBarabasiAlbertGraph(int n, int outdegree, long seed) {
	int[] inlinks = new int[n];
	int[] outlinks = new int[n];
	int[][] ancestors = null;
	
	int[][] successors = new int[n][outdegree];
	int poolSize = 2 * (n - outdegree) * outdegree;
	int[] pool = new int[poolSize];
	int[] chosen = new int[outdegree];
	Random random = new Random(seed);
	int pos = 0;
	// initialize
	for (int i = 0; i < outdegree; i++) {
		chosen[i] = i;
	}
	
	int source = outdegree;
	while (source < n) {
		// links to chosen
		for (int i = 0; i < outdegree; i++) {
			successors[source][i] = chosen[i];
		}
		// chosen chunk
		for (int i = 0; i < outdegree; i++) {
			pool[pos + i] = chosen[i];
		}
		pos = pos + outdegree;
		
		// source copies chunk 
		for (int i = 0; i < outdegree; i++) {
			pool[pos + i] = source;
		}
		pos = pos + outdegree;
		
		for (int i = 0; i < outdegree; i++) {
			int sample = random.nextInt(pos);
			chosen[i] = pool[sample];
		}
		source++;
	}
	
	ancestors = IntArrays.reverse(successors);
	for (int i = 0; i < n; i++) {
		inlinks[i] = ancestors[i].length;
		outlinks[i] = successors[i].length;
	} 
	return new WebGraph(inlinks, ancestors, outlinks);
}


// TOO SLOW!!!
// 	 * In fact we use the formula
//	 * <code>
// 	 * p = (indegree(v) + 1) / (|E| + |V|)
// 	 * </code>
// 	 * for computing probability <code>p</code> of linking a new node to an
// 	 * already existing node <code>v</code>. Here <code>|E|</code> and
// 	 * <code>|V|</code> is the number of edges and vertices respectively in
// 	 * under-construction graph.
// 	 *
//	public  static WebGraph newBarabasiAlbertGraph(int n, int outdegree, int initial, long seed) {
// 		int links = n * outdegree;
// 		int vertices = initial;
// 		int edges = 0;
// 		int[] sources = new int[links];
// 		int[] targets = new int[links];
// 		int[][] successors = new int[n][outdegree];
//		
// 		int[] inlinks = new int[n];
// 		int[] outlinks = new int[n];
// 		int[][] ancestors = new int[n][];
// 		Random random = new Random(seed);
// 		for (int i = initial; i < n; i++) {
// 			int m = 0;
// 			while (m < outdegree) {
// 				int candidate = random.nextInt(vertices);
// 				double probability = (inlinks[candidate] + 1.0) / (vertices + edges);
// 				double threshold = random.nextDouble();
// 				if (probability > threshold) {
// 					outlinks[i]++;
// 					inlinks[candidate]++;
// 					successors[i][m] = candidate;
// 					sources[edges] = i;
// 					targets[edges] = candidate;
// 					edges++;
// 					m++;
// 				}
// 			}
// 			vertices++;
// 		}
// 		// Compute ancestors[][]
// 		for (int i = 0; i < n; i++) {
// 			int indegree = inlinks[i];
// 			ancestors[i] = new int[indegree];
// 		}
// 		int[] counters = new int[n];
// 		for (int i = 0; i < edges; i++) {
// 			int target = targets[i];
// 			ancestors[target][counters[target]] = sources[i];
// 			counters[target]++;
// 		}
// 		return new WebGraph(inlinks, ancestors, outlinks);		
// 	}
		
	
	/**
	 * Returns a random graph with specified numbers of pages, outlinks per
	 * page.
	 * 
	 * @param n number of pages
	 * @param outdegree number of outlinks per page
	 * @return random graph
	 */
	public static WebGraph newRandomGraph(int n, int outdegree) {
		return newRandomGraph(n, outdegree, 100L);
    }

	
	/**
	 * Returns a random graph with specified numbers of pages, outlinks per
	 * page and random number generator seed.
	 * 
	 * @param n number of pages
	 * @param outdegree number of outlinks per page
	 * @param seed seed for random number generator
	 * @return random graph
	 */	
	public static WebGraph newRandomGraph(int n, int outdegree, long seed) {
		int[][] successors = new int[n][];
		
		// Allocate inlinks[], outlinks[]
		int[] outlinks = new int[n];
		int[] inlinks = new int[n];
		
		int[][] ancestors = null;
		
		Random random = new Random(seed);
		for (int i = 0; i < n; i++) {
			successors[i] = new int[outdegree];
			for (int j = 0; j < outdegree; j++) { 
				int target = random.nextInt(n);
				successors[i][j] = target;
			}
		}
		// Compute inlinks[], ancestors[][], outlinks[]
		ancestors = IntArrays.reverse(successors);
		for (int i = 0; i < n; i++) {
			inlinks[i] = ancestors[i].length;
			outlinks[i] = successors[i].length;
		} 
		return new WebGraph(inlinks, ancestors, outlinks);
	}	
	
	/**
	 * Returns a Nearly Completely Decomposable (NCD) random graph with specified numbers of pages, outlinks per
	 * page, number of blocks and lower bound of probability of belonging to
	 * block along the diagonal
	 * 
	 * @param n number of pages
	 * @param outdegree number of outlinks per page
	 * @param blocks number of blocks
	 * @param prob lower bound of probability of belonging to
	 * block along the diagonal
	 * @return NCD  random graph
	 */		
	public static WebGraph newNCDRandomGraph(int n, int outdegree, int blocks, double prob) {
		double[] probs = new double[blocks];
		for (int i = 0; i < blocks; i++) {
			probs[i] = prob;
		}
		return newNCDRandomGraph(n, outdegree, blocks, probs);
	}
	
	/**
	 * Returns a Nearly Completely Decomposable (NCD) random graph with specified numbers of pages, outlinks per
	 * page, number of blocks and lower bounds of probability of belonging to
	 * blocks along the diagonal
	 * 
	 * @param n number of pages
	 * @param outdegree number of outlinks per page
	 * @param blocks number of blocks
	 * @param probs lower bounds of probability of belonging to
	 * blocks along the diagonal
	 * @return NCD  random graph
	 */			
	public static WebGraph newNCDRandomGraph(int n, int outdegree, int blocks, double[] probs) {
		int[][] limits = IntArrays.partLimits(n, blocks, 0);
		return newNCDRandomGraph(n, outdegree, limits, probs);
	}	
	
	/**
	 * Returns a Nearly Completely Decomposable (NCD) random graph with specified numbers of pages, outlinks per
	 * page, block sizes and lower bounds of probability of belonging to
	 * blocks along the diagonal.
	 * 
	 * @param n number of pages
	 * @param outdegree number of outlinks per page
	 * @param sizes number of block sizes
	 * @param probs lower bounds of probability of belonging to
	 * blocks along the diagonal
	 * @return NCD  random graph
	 */		
	public static WebGraph newNCDRandomGraph(int n, int outdegree, int sizes[], double[] probs) {
		int blocks = sizes.length;
		int[][] limits = new int[blocks][2];
		int cursor = 0;
		for (int i = 0; i < blocks; i++) {
			limits[i][0] = cursor;
			limits[i][1] = cursor + sizes[i];
			cursor = cursor + sizes[i]; 
		}		
		return newNCDRandomGraph(n, outdegree, limits, probs);	
	}
	
	/**
	 * Returns a Nearly Completely Decomposable (NCD) random graph with specified numbers of pages, outlinks per
	 * page, limits for blocks  and lower bounds of probability of belonging to
	 * blocks along the diagonal.
	 * 
	 * @param n number of pages
	 * @param outdegree number of outlinks per page
	 * @param limits limits for blocks
	 * @param probs lower bounds of probability of belonging to
	 * blocks along the diagonal
	 * @return NCD  random graph
	 */		
	public static WebGraph newNCDRandomGraph(int n, int outdegree, int limits[][], double[] probs) {
		int m = limits.length;
		int links = n * outdegree;
		int[] sources = new int[links];
		int[] targets = new int[links];
		int[][] successors = new int[n][];
		
		// Allocate inlinks[], outlinks[]
		int[] outlinks = new int[n];
		int[] inlinks = new int[n];
		int[][] ancestors = null;
		
		// XXX make seed a parameter
		Random random = new Random(100L);
		for (int i = 0; i < m; i++) {
			int start = limits[i][0];
			int end = limits[i][1];
			double threshold = probs[i];
			for(int j = start; j < end; j++) {
				successors[j] = new int[outdegree];
				for (int k = 0; k < outdegree; k++) {
					double sample = random.nextDouble();
					int target;
					if (sample > threshold) {
						target = random.nextInt(n);
					} else {
						target = start + random.nextInt(end - start);
					}
					successors[j][k] = target;
				}
			}
		}
		
		// Compute inlinks[], ancestors[][], outlinks[]
		ancestors = IntArrays.reverse(successors);
		for (int i = 0; i < n; i++) {
			inlinks[i] = ancestors[i].length;
			outlinks[i] = successors[i].length;
		} 
		return new WebGraph(inlinks, ancestors, outlinks);	
	}
}

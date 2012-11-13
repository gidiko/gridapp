package webmatrix;

import java.util.Vector;
import java.util.Arrays;
import java.io.*;
import webmatrix.io.*;
import webmatrix.util.*;


public class WebGraph extends Graphlet {
    
    public WebGraph(int[] inlinks, int[][] ancestors, int[] outlinks) {
	super(0, inlinks, ancestors, outlinks); 
    }

    
    public WebGraph(Graphlet graphlet) {
	this(graphlet.inlinks, graphlet.ancestors, graphlet.outlinks);
    }
    
    /**
     * Returns a webgraph with "inverted" link directions.
     * Ancestors become successors and vice versa.	This would correspond to a
     * transpose() operation in a matrix representation of our graph.
     *
     * @return webgraph with "inverted" link directions.
     */         
    public WebGraph transpose() {
	int[][] successors = IntArrays.reverse(ancestors);
	return new WebGraph(outlinks, successors, inlinks); 
    }
    
	
    /**
     * Compresses webgraph to a new size.
     *
     * @param m size of the compressed graph.
     * This is just the number of the nodes (pages) it contains.
     * @return compressed webgraph.
     */      	
    public WebGraph compress(int m) {
	int[][] limits = IntArrays.partLimits(size, m, start);
	return compress(limits);
    }
	
	
    /**
     * Compresses webgraph into successive parts with given sizes. 
     *
     * @param sizes sizes of parts to compress.
     * @return compressed webgraph.
     */       
    public WebGraph compress(int[] sizes) {
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
     * Compresses webgraph into parts defined by successive page ranges. 
     * Limits for these ranges are given as <code>[limits[][0],
     * limits[][1])</code>. Pages within such a range are collapsed to a single
     * page in the compressed graph. 
     * 
     * @param limits limits for the ranges of webgraph pages to compress.
     * @return compressed webgraph.	 
     */   
    public WebGraph compress(int limits[][]) {
	int m = limits.length;
	int[] inlinksCompressed = new int[m];
	int[] outlinksCompressed = new int[m];
	int[][] ancestorsCompressed = new int[m][];
		
	// map indices in the original graph to indices in the compressed graph
	int[] map = new int[n];
	int counter;
	counter = 0;
	for (int ii = 0; ii < m; ii++) {
	    int end = limits[ii][1];
	    while (counter < end) {
		map[counter] = ii;
		counter++;
	    }
	}
	for (int ii = 0; ii < m; ii++) {
	    int start = limits[ii][0];
	    int end = limits[ii][1];
	    inlinksCompressed[ii] = 0;
	    outlinksCompressed[ii] = 0;
	    for (int j = start; j < end; j++) {
		inlinksCompressed[ii] = inlinksCompressed[ii] + inlinks[j];
		outlinksCompressed[ii] = outlinksCompressed[ii] + outlinks[j];
	    }
	    int indegree = inlinksCompressed[ii];
	    ancestorsCompressed[ii] = new int[indegree];
	    counter = 0;
	    int length;
	    for (int j = start; j < end; j++) {
		length = ancestors[j].length;
		for (int k = 0; k < length; k++) {
		    ancestorsCompressed[ii][counter] = map[ancestors[j][k]];
		    counter++;
		}
	    }
	}
	return new WebGraph(inlinksCompressed, ancestorsCompressed, outlinksCompressed);
    }
	
	
    /**
     * Returns an array of diagonal blocks of a webgraph.
     * Each block is a new webgraph.
     *
     * @param m number of diagonal blocks.
     * @return array of diagonal blocks.
     */      	
    public WebGraph[] diagonalBlocks(int m) {
	int[][] limits = IntArrays.partLimits(size, m, start);
	return diagonalBlocks(limits);
    }
	
	
    /**
     * Returns an array of diagonal blocks of a webgraph.
     * Each block is a new webgraph.
     *
     * @param sizes sizes of diagonal blocks.
     * @return array of diagonal blocks.
     */       
    public WebGraph[] diagonalBlocks(int[] sizes) {
	int m = sizes.length;
	int[][] limits = new int[m][2];
	int cursor = start;
	for (int i = 0; i < m; i++) {
	    limits[i][0] = cursor;
	    limits[i][1] = cursor + sizes[i];
	    cursor = cursor + sizes[i]; 
	}
	return diagonalBlocks(limits);
    }
		
	
    /**
     * Returns an array of diagonal blocks of a webgraph with known
     * <code>limits</code>.
     * Each block is a new webgraph.
     *
     * @param limits limits of diagonal blocks within original webgraph. 
     * @return array of diagonal blocks.
     */       	
    public WebGraph[] diagonalBlocks(int limits[][]) {
	int m = limits.length;
	WebGraph[] graphs = new WebGraph[m];
	for (int i = 0; i < m; i++) {
	    graphs[i] = diagonalBlock(limits[i]);
	}
	return graphs;
    }
	
	
    /**
     * Returns  diagonal block of a webgraph with known
     * <code>limits</code>.
     * 
     * @param limits limits of diagonal block. 
     * @return diagonal block.
     */       	
    public WebGraph diagonalBlock(int limits[]) {
	WebGraph graph = null;
	int start = limits[0];
	int end = limits[1];
	int size = end - start;
	int lower = start;
	int upper = end - 1;
	int[] inlinks = new int[size];
	int[][] ancestors = new int[size][];
	int[] outlinks = new int[size];
		
	for (int j = 0; j < size; j++) {
	    int k = start + j;
	    int[] displaced = IntArrays.filterByBounds(this.ancestors[k], lower, upper);
	    ancestors[j] = IntArrays.plus(displaced, -start);
	}
	int[][] successors = IntArrays.reverse(ancestors);
	for (int j = 0; j < size; j++) {
	    inlinks[j] = ancestors[j].length;
	    outlinks[j] = successors[j].length;
	}
	graph = new WebGraph(inlinks, ancestors, outlinks);
	return graph;
    }
		
	
    /**
     * Returns an array of diagonal blocks of a webgraph suitably semi-framed by
     * rest webgraph compressed info.
     * Each block is a new webgraph.
     *
     * @param m number of diagonal blocks.
     * @return array of diagonal blocks.
     */      	
    public WebGraph[] diagonalFramedBlocks(int m) {
	int[][] limits = IntArrays.partLimits(size, m, start);
	return diagonalFramedBlocks(limits);
    }
	
	
    /**
     * Returns an array of diagonal blocks of a webgraph suitably semi-framed by
     * rest webgraph compressed info.
     * Each block is a new webgraph.
     *
     * @param sizes sizes of diagonal blocks.
     * @return array of diagonal blocks.
     */       
    public WebGraph[] diagonalFramedBlocks(int[] sizes) {
	int m = sizes.length;
	int[][] limits = new int[m][2];
	int cursor = start;
	for (int i = 0; i < m; i++) {
	    limits[i][0] = cursor;
	    limits[i][1] = cursor + sizes[i];
	    cursor = cursor + sizes[i]; 
	}
	return diagonalFramedBlocks(limits);
    }
	

    /**
     * Returns an array of diagonal blocks of a webgraph with known
     * <code>limits</code> suitably semi-framed by
     * rest webgraph compressed info.
     * Each block is a new webgraph.
     *
     * @param limits limits of diagonal blocks within original webgraph. 
     * @return array of diagonal blocks.
     */       	
    public WebGraph[] diagonalFramedBlocks(int limits[][]) {
	int m = limits.length;
	WebGraph[] graphs = new WebGraph[m];
		
	for (int i = 0; i < m; i++) {
	    graphs[i] = diagonalFramedBlock(limits[i]);
	}
	return graphs;
    }
	
	
    /**
     * Returns diagonal block of a webgraph with known
     * <code>limits</code> suitably semi-framed by
     * rest webgraph compressed info.
     * 
     * @param limits limits of diagonal block within original webgraph. 
     * @return diagonal block.
     */       	
    public WebGraph diagonalFramedBlock(int limits[]) {
		
	WebGraph graph = null;
		
	int start = limits[0];
	int end = limits[1];
	int size = end - start;
	int lower = start;
	int upper = end - 1;
	// +1 accounts for the slim (width=1) semi-frame
	int[] inlinks = new int[size + 1];
	int[][] ancestors = new int[size + 1][];
	int[] outlinks = new int[size + 1];
		
	ancestors[size] = IntArrays.make(0);
	// before block
	for (int ii = 0; ii < start; ii++) {
	    int numOffBounds = IntArrays.filterByOffBounds(this.ancestors[ii], lower, upper).length;
	    int[] displaced = IntArrays.filterByBounds(this.ancestors[ii], lower, upper);
	    ancestors[size] = IntArrays.append(ancestors[size], IntArrays.plus(displaced, -start));
	    ancestors[size] = IntArrays.append(ancestors[size], IntArrays.repeat(size, numOffBounds));
	}
			
	// within block
	for (int ii = start; ii < end; ii++) {
	    int jj = ii - start;
	    int numOffBounds = IntArrays.filterByOffBounds(this.ancestors[ii], lower, upper).length;
	    int[] displaced = IntArrays.filterByBounds(this.ancestors[ii], lower, upper);
	    ancestors[jj] = IntArrays.plus(displaced, -start);
	    ancestors[jj] = IntArrays.append(ancestors[jj], IntArrays.repeat(size, numOffBounds));
	}			
		
	// after block
	for (int ii = end; ii < n; ii++) {
	    int numOffBounds = IntArrays.filterByOffBounds(this.ancestors[ii], lower, upper).length;
	    int[] displaced = IntArrays.filterByBounds(this.ancestors[ii], lower, upper);
	    ancestors[size] = IntArrays.append(ancestors[size], IntArrays.plus(displaced, -start));
	    ancestors[size] = IntArrays.append(ancestors[size], IntArrays.repeat(size, numOffBounds));
	}
		
	int[][] successors = IntArrays.reverse(ancestors);
	int num = size + 1;
	for (int j = 0; j < num; j++) {
	    inlinks[j] = ancestors[j].length;
	    outlinks[j] = successors[j].length;
	}
	graph = new WebGraph(inlinks, ancestors, outlinks);
	return graph;
    }
	
	
    public double inSelf(int[] limits) {
	double index = 0.0;
	int start = limits[0];
	int end = limits[1];
	int size = end - start;
	int lower = start;
	int upper = end - 1;	
	for (int j = 0; j < size; j++) {
	    int k = start + j;
	    int[] selfAncestors = IntArrays.filterByBounds(this.ancestors[k], lower, upper);
	    int num = selfAncestors.length;
	    for (int i = 0; i < num; i++) {
		index = index + 1.0 / (double)outlinks[selfAncestors[i]];
	    }
	}	
	return index;	
    }
	
	
    public double outSelf(int[] limits) {
	double index = 0.0;
		
	int[][] successors = IntArrays.reverse(ancestors);
	int start = limits[0];
	int end = limits[1];
	int size = end - start;
	int lower = start;
	int upper = end - 1;
	for (int j = 0; j < size; j++) {
	    int k = start + j;
	    int[] selfSuccessors = IntArrays.filterByBounds(successors[k], lower, upper);
	    int num = selfSuccessors.length;
	    index = index + (double)num / (double)outlinks[k];
	}
	return index;		
    }
	
	
    public double inoutSelf(int[] limits) {
	double index = inSelf(limits) - outSelf(limits);
	return index;
    }
	
	
    public double inOthers(int[] limits) {
	double index = 0.0;
		
	int start = limits[0];
	int end = limits[1];
	int size = end - start;
	int lower = start;
	int upper = end - 1;	
	for (int j = 0; j < size; j++) {
	    int k = start + j;
	    int[] otherAncestors = IntArrays.filterByOffBounds(this.ancestors[k], lower, upper);
	    int num = otherAncestors.length;
	    for (int i = 0; i < num; i++) {
		index = index + 1.0 / (double)outlinks[otherAncestors[i]];
	    }
	}	
	return index;
    }
	
	
    public double outOthers(int[] limits) {
	double index = 0.0;
		
	int[][] successors = IntArrays.reverse(ancestors);
	int start = limits[0];
	int end = limits[1];
	int size = end - start;
	int lower = start;
	int upper = end - 1;
	for (int j = 0; j < size; j++) {
	    int k = start + j;
	    int[] otherSuccessors = IntArrays.filterByOffBounds(successors[k], lower, upper);
	    int num = otherSuccessors.length;
	    index = index + (double)num / (double)outlinks[k];
	}
	return index;		
    }
	
	
    public double inoutOthers(int[] limits) {
	double index = inOthers(limits) - outOthers(limits);
	return index;
    }
		
	
	
    public WebGraph[] diagonalFramedBlocksQuick(int m) {
	int[][] limits = IntArrays.partLimits(size, m, start);
	return diagonalFramedBlocksQuick(limits);
    }
	
    
    public WebGraph[] diagonalFramedBlocksQuick(int[] sizes) {
	int m = sizes.length;
	int[][] limits = new int[m][2];
	int cursor = start;
	for (int i = 0; i < m; i++) {
	    limits[i][0] = cursor;
	    limits[i][1] = cursor + sizes[i];
	    cursor = cursor + sizes[i]; 
	}
	return diagonalFramedBlocksQuick(limits);
    }
	

    public WebGraph[] diagonalFramedBlocksQuick(int limits[][]) {
	int m = limits.length;
	WebGraph[] graphs = new WebGraph[m];
	int[][] blockAncestors = new int[n][];
	int[][] offBlockSuccessors = new int[n][];
	int[][] allSuccessors = IntArrays.reverse(this.ancestors);
		
	int[] ancestorsOffBounds = new int[n];
	int[] successorsOffBounds = new int[n];
		
	int[] inlinks = new int[size + 1];
	int[][] ancestors = new int[size + 1][];
	int[] outlinks = new int[size + 1];
		
		
	for (int i = 0; i < m; i++) {
	    int start = limits[i][0];
	    int end = limits[i][1];
	    int size = end - start;
	    int lower = start;
	    int upper = end - 1;
	    int[] displaced = null;
			
	    // fill (block -> block), (offblock -> block)
	    for (int j = 0; j < size; j++) {
		int k = start + j;
		displaced = IntArrays.filterByBounds(this.ancestors[k], lower, upper);
		blockAncestors[k] = IntArrays.plus(displaced, -start);
				
		displaced = IntArrays.filterByOffBounds(allSuccessors[k], lower, upper);
		offBlockSuccessors[k] = IntArrays.plus(displaced, -start);
				
		ancestorsOffBounds[k] = this.ancestors[k].length - blockAncestors[k].length;
		successorsOffBounds[k] = allSuccessors[k].length - offBlockSuccessors[k].length;
				
	    }
	}
	for (int i = 0; i < m; i++) {
	    int start = limits[i][0];
	    int end = limits[i][1];
	    int size = end - start;
	    int lower = start;
	    int upper = end - 1;
			
	    // (all -> block) number of links
	    int linkCounter = 0;
			
	    // fill (block -> offblock)
	    for (int j = 0; j < size; j++) {
		int k = start + j;
		ancestors[j] = IntArrays.append(blockAncestors[k], IntArrays.repeat(size, ancestorsOffBounds[k]));
		linkCounter = linkCounter + ancestors[j].length + successorsOffBounds[k];
	    }
	    ancestors[size] = IntArrays.make(0);
	    for (int j = 0; j < size; j++) {
		int k = start + j;
		ancestors[size] = IntArrays.append(ancestors[size], IntArrays.repeat(j,  successorsOffBounds[k]));
	    }
			
	    // fill (offblock -> offblock)
	    int remaining = this.links - linkCounter;
	    ancestors[size] = IntArrays.append(ancestors[size], IntArrays.repeat(size, remaining)); 
			
	    // final step
	    int[][] successors = IntArrays.reverse(ancestors);
	    int num = size + 1;
	    for (int j = 0; j < num; j++) {
		inlinks[j] = ancestors[j].length;
		outlinks[j] = successors[j].length;
	    }
		
	    graphs[i] = new WebGraph(inlinks, ancestors, outlinks);
	}
	return graphs;
    }
	

	
    /**
     * Compresses a number of nodes at graph's tail.
     * Tail nodes are highest index ones.
     *
     * @param num  number of nodes at  graph's tail.
     */   
    public WebGraph compressTail(int num) {
	// first size - num nodes will remain the same
	int same = size - num;
	int m = same + 1;
	int[][] limits = new int[m][2];
	for (int i = 0; i < same; i++) {
	    limits[i][0] = i;
	    limits[i][1] = i + 1;
	}
	limits[same][0] = same;
	limits[same][1] = size;
	return compress(limits);
    }
	
	
    /**
     * Permutes nodes of a web graph according to given permutation vector
     *(<code>map</code>).
     * page[i] <- page[map[i]].  
     *
     * @param map permutation vector
     * @return web graph with permutation applied
     */
    public WebGraph permute(int[] map) {
	int n = map.length;
	// make inverse map
	int[] imap = new int[n];
	for (int i = 0;  i < n; i++) {
	    imap[map[i]] = i;
	} 
	int[] inlinksPermuted = new int[n];
	int[][] ancestorsPermuted = new int[n][];
	int[] outlinksPermuted = new int[n];
		
	for (int i = 0; i < n; i++) {
	    inlinksPermuted[i] = inlinks[map[i]];
	    int indegree = inlinksPermuted[i];
	    ancestorsPermuted[i] = new int[indegree];
	    System.arraycopy(ancestors[map[i]], 0, ancestorsPermuted[i], 0, indegree);
	    for (int j = 0; j < indegree; j++) {
		int value = ancestorsPermuted[i][j];
		ancestorsPermuted[i][j] = imap[value];
	    } 
	    outlinksPermuted[i] = outlinks[map[i]];
	}
	return new WebGraph(inlinksPermuted, ancestorsPermuted, outlinksPermuted);
    }
	
    
    
    
    
	
    public int[] computeRowIndices() {
	int nnz = multilinks;
	int[] icoord = new int[nnz];
	int counter = 0;
	for (int i = 0; i < size; i++) {
	    int[] condensed = IntArrays.removeDuplicates(ancestors[i]);
	    int num = condensed.length;
	    for (int j = 0; j < num; j++) {
		icoord[counter] = i;
		counter++;
	    }
	}
	return icoord;
    }
	
	
    public int[] computeColumnIndices() {
	int nnz = multilinks;
	int[] jcoord = new int[nnz];
	int counter = 0;
	for (int i = 0; i < size; i++) {
	    int[] condensed = IntArrays.removeDuplicates(ancestors[i]);
	    int num = condensed.length;
	    for (int j = 0; j < num; j++) {
		jcoord[counter] = condensed[j];
		counter++;
	    }
	}
	return jcoord;	
    }
	
	


    public double[] computeData() {
	int nnz = multilinks;
	double[] data = new double[nnz];
	int counter = 0;
	for (int i = 0; i < size; i++) {
	    int[] condensed = IntArrays.removeDuplicates(ancestors[i]);
	    int num = condensed.length;
	    int[] counts = new int[num];
	    for (int j = 0; j < num; j++) {
		counts[j] = IntArrays.count(ancestors[i], condensed[j]);
	    }
	    int total = IntArrays.sum(counts);
	    for (int j = 0; j < num; j++) {
		data[counter] = (double)counts[j] / (double)total;
		counter++;
	    }
	}
	return data;
    }
	
    public WebGraph neutralizeNodes(int[] index) {
	int size = index.length;
	int[][] ancestors = IntArrays.copy(this.ancestors);
	for (int i = 0; i < size; i++) {
	    ancestors[index[i]] = IntArrays.make(0);
	}
	int[][] successors = IntArrays.reverse(ancestors);
	for (int j = 0; j < n; j++) {
	    inlinks[j] = ancestors[j].length;
	    outlinks[j] = successors[j].length;
	}
	return new WebGraph(inlinks, ancestors, outlinks);
    }

    /**
     * Loads webgraph from binary file.
     *
     * @param filename  name of file.
     * @throws IOException if file cannot be read.
     */     
    public static WebGraph load(String filename) throws IOException {
	return new WebGraph(Graphlet.load(filename));
    }
	
	
}


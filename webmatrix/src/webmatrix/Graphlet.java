package webmatrix;

import java.util.Random;
import java.util.Vector;
import java.util.Enumeration;
import java.io.*;
import webmatrix.util.*;

/**
 * Fragment of a web link structure consisting of a set of consecutive pages.
 */
public class Graphlet implements Serializable {

    // To be serialized

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
     * first page of this fragment; fragment includes <code>size</code>
     * consecutive pages
     */
    int start;

    /**
     * first page of next fragment.
     */
    int end;

    /**
     * number of all pages in the graph.
     */
    int n;

    /**
     * number of inbound links for fragment pages.
     */
    int[] inlinks;

    /**
     * pages pointing to fragment pages.
     */
    int[][] ancestors;

    /**
     * number of outbound links for all pages.
     */
    int[] outlinks;

    ////////////////////////////////////////
    // Not to be serialized
    ////////////////////////////////////////
    /**
     * number of dangling pages.
     */
    int danglingNum;

    /**
     * dangling pages.
     */
    int[] danglingPages;

    /**
     * number of fragment pages, i.e. <code>end - start</code>
     */
    int size;

    /**
     * number of fragment links
     */
    int links;

    /**
     * number of fragment links but here multiple links from a page to another
     * is counted only once
     */
    int multilinks;

    /**
     * percentage of fragment links with respect to <code>size * n</code>
     */
    double sparsity;

    /**
     * Construct a graph fragment with given starting page, number of inbound
     * links, inbound linking pages and number of outbound links.
     *
     * @param  start first page of this fragment; fragment includes <code>size</code>
     * consecutive pages.
     * @param  inlinks number of inbound links for fragment pages.
     * @param  ancestors pages pointing to fragment pages.
     * @param  outlinks number of outbound links for all pages.
     */
    public Graphlet(int start, int[] inlinks, int[][] ancestors, int[] outlinks) {
        this(start, start + inlinks.length, outlinks.length, inlinks, ancestors, outlinks);
    }

    /**
     * Construct a graph fragment with given <code>[start, end)</code> range of
     * pages, total number of pages, number of inbound links, inbound linking
     * pages and number of outbound links.
     *
     * @param  start first page of this fragment; fragment includes <code>size</code>
     * consecutive pages.
     * @param  end first page of next fragment.
     * @param  n number of all pages in the graph.
     * @param  inlinks number of inbound links for fragment pages.
     * @param  ancestors pages pointing to fragment pages.
     * @param  outlinks number of outbound links for all pages.
     */
    public Graphlet(int start, int end, int n, int[] inlinks, int[][] ancestors, int[] outlinks) {

        this.n = n;
        this.start = start;
        this.end = end;

        // Shallow copies
        this.inlinks = inlinks;
        this.ancestors = ancestors;
        this.outlinks = outlinks;

        size = end - start;
        links = 0;
        for (int i = 0; i < size; i++) {
            links = links + inlinks[i];
        }
	//[
	multilinks = computeMultilinks();
        sparsity = (double)multilinks / size;
	sparsity = sparsity / n;

        // Compute danglingNum, danglingPages[]
        Vector<Integer> danglingPagesVector = new Vector<Integer>();
        for (int i = 0; i < n; i++) {
            if (outlinks[i] == 0) {
                danglingPagesVector.add(i);
            }
        }
	//[
	danglingPages = IntArrays.toArray(danglingPagesVector);
	danglingNum = danglingPages.length;
    }

    /**
     * Steps pagerank calculation for vector <code>x</code>.
     * Teleportation parameter <code>alpha = 0.85</code> and uniform preferences vector.
     *
     * @param x  start vector for the step.
     * @return resulting vector fragment.
     */
    public Ranklet mult(Ranklet x) {
        double alpha = 0.85;
        return mult(x, alpha);
    }

    /**
     * Steps pagerank calculation for vector <code>x</code> and parameter <code>alpha</code>.
     * Uniform preferences vector.
     *
     * @param x  start vector for the step.
     * @param  alpha teleportation parameter.
     * @return resulting vector fragment.
     */
    public Ranklet mult(Ranklet x, double alpha) {
	double weight = 1.0 / n;
	double[] my = DoubleArrays.repeat(weight, n);
	return mult(x, alpha, my);
    }

    /**
     * Steps pagerank calculation for vector <code>x</code>, parameter
     * <code>alpha</code> and given preferences vector.
     *
     * @param x  start vector for the step.
     * @param alpha  teleportation parameter.
     * @param my  preferences vector.
     * @return resulting vector fragment.
     */
    public Ranklet mult(Ranklet x, double alpha, double[] my) {
        double[] vector = x.getData();
        double[] result = new double[size];

	double weight = alpha / n;
	double danglingTerm = 0.0;
	for (int k = 0; k < danglingNum; k++) {
	    int page = danglingPages[k];
	    danglingTerm = danglingTerm + weight * vector[page];
	}

        for (int i = 0; i < size; i++) {
            int indegree = inlinks[i];
            double linkTerm = 0.0;
	    for (int j = 0; j < indegree; j++) {
                int k = ancestors[i][j];
                // Since k is an ancestor, its outdegree is not 0
                linkTerm = linkTerm + alpha * vector[k] / outlinks[k];
            }
	    double myTerm = (1.0 - alpha) * my[i];
	    result[i] = linkTerm + danglingTerm + myTerm;
        }
        return new Ranklet(start, result);
    }


    /**
     * Returns an array of graphlets arising when breaking this graphlet into
     * <code>m</code> parts.
     *
     * @param m number of parts.
     * @return array of graphlets.
     */
    public Graphlet[] split(int m) {
	int[][] limits = IntArrays.partLimits(size, m, start);
	return split(limits);
    }


    /**
     * Returns an array of graphlets arising when breaking this graphlet into
     * parts with known <code>sizes</code>.
     *
     * @param m sizes of parts.
     * @return array of graphlets.
     */
    public Graphlet[] split(int[] sizes) {
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


    /**
     * Returns an array of graphlets arising when breaking this graphlet into
     * parts with known <code>limits</code>.
     *
     * @param limits limits of parts.
     * @return array of graphlets.
     */
    public Graphlet[] split(int[][] limits) {
	int m = limits.length;
	Graphlet[] graphlets = new Graphlet[m];
	for (int i = 0; i < m; i++) {
	    int start = limits[i][0];
	    int end = limits[i][1];
	    int size = end - start;
	    int[] inlinks = new int[size];
	    int[][] ancestors = new int[size][];
	    int[] outlinks = new int[n];
	    for (int j = 0; j < size; j++) {
		int k = start + j;
		inlinks[j] = this.inlinks[k];
		int indegree = this.ancestors[k].length;
		ancestors[j] = new int[indegree];
		System.arraycopy(this.ancestors[k], 0, ancestors[j], 0, indegree);
	    }
	    System.arraycopy(this.outlinks, 0, outlinks, 0, n);
	    graphlets[i] = new Graphlet(start, inlinks, ancestors, outlinks);
	}
	return graphlets;
    }


    public Graphlet horizontalStripe(int[] limits) {
	int start = limits[0];
	int end = limits[1];
	int size = end - start;
	int[] inlinks = new int[size];
	int[][] ancestors = new int[size][];
	int[] outlinks = new int[n];
	for (int j = 0; j < size; j++) {
	    int k = start + j;
	    inlinks[j] = this.inlinks[k];
	    int indegree = this.ancestors[k].length;
	    ancestors[j] = new int[indegree];
	    System.arraycopy(this.ancestors[k], 0, ancestors[j], 0, indegree);
	}
	System.arraycopy(this.outlinks, 0, outlinks, 0, n);
	return new Graphlet(start, inlinks, ancestors, outlinks);
    }


    public static void pack(Graphlet[] graphlets) {

    }

    /**
     * Stores graph fragment to binary file.
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
     * <td><code>start</code></td>
     * <td>start row in <code>[start, end)</code></td>
     * </tr>
     * <tr>
     * <td><code>int</code></td>
     * <td><code>end</code></td>
     * <td>end row in <code>[start, end)</code></td>
     * </tr>
     * <tr>
     * <td><code>int</code></td>
     * <td><code>n</code></td>
     * <td>number of columns</td>
     * </tr>
     * </tr>
     * <tr>
     * <td><code>int[end - start]</code></td>
     * <td><code>inlinks</code></td>
     * <td>indegrees for included nodes</td>
     * </tr>
     * <tr>
     * <td><code>int[end - start][]</code></td>
     * <td><code>ancestors</code></td>
     * <td>ancestors for included nodes</td>
     * </tr>
     * <tr>
     * <td><code>int[n]</code></td>
     * <td><code>outlinks</code></td>
     * <td>outdegrees for all nodes</td>
     * </tr>
     * </table>
     * </p>
     * <p>
     * Note that the following fiels are not dumped to file although they are
     * part of the state for a graphlet object:
     * </p>
     * <p>
     * <table border="1">
     * <tr>
     * <td><b>datatype</b></td>
     * <td><b>name</b></td>
     * <td><b>description</b></td>
     * </tr>
     * <tr>
     * <td><code>int</code></td>
     * <td><code>danglingNum</code></td>
     * <td>number of dangling pages</td>
     * </tr>
     * <tr>
     * <td><code>int[]</code></td>
     * <td><code>danglingPages</code></td>
     * <td>dangling pages</td>
     * </tr>
     * <tr>
     * <td><code>int</code></td>
     * <td><code>size</code></td>
     * <td><code>end - start</code></td>
     * </tr>
     * <tr>
     * <td><code>int</code></td>
     * <td><code>links</code></td>
     * <td>number of all links</td>
     * </tr>
     * <tr>
     * <td><code>double</code></td>
     * <td><code>sparsity</code></td>
     * <td><code>(size * n) / links</code></td>
     * </tr>
     * </table>
     * </p>
     * @param filename  name of file.
     * @throws IOException if file cannot be written.
     */
    public void dump(String filename) throws IOException {
        DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filename)));
	dos.writeInt(start);
        dos.writeInt(end);
        dos.writeInt(n);
	for (int i = 0; i < size; i++) {
            dos.writeInt(inlinks[i]);
        }
        for (int i = 0; i < size; i++) {
            int indegree = inlinks[i];
            for (int j = 0; j < indegree; j++) {
                dos.writeInt(ancestors[i][j]);
            }
        }
        for (int i = 0; i < n; i++) {
            dos.writeInt(outlinks[i]);
        }
        dos.close();
    }

    /**
     * Loads graph fragment from binary file.
     *
     * @param filename  name of file.
     * @throws IOException if file cannot be read.
     */
    public static Graphlet load(String filename) throws IOException {
        DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(filename)));
	int start = dis.readInt();
        int end = dis.readInt();
        int n = dis.readInt();
	int size = end - start;

        int[] inlinks = new int[size];
        int[][] ancestors = new int[size][];
        int[] outlinks = new int[n];

        for (int i = 0; i < size; i++) {
            inlinks[i] = dis.readInt();
        }
        for (int i = 0; i < size; i++) {
            int indegree = inlinks[i];
            ancestors[i] = new int[indegree];
            for (int j = 0; j < indegree; j++) {
                ancestors[i][j] = dis.readInt();
            }
        }
        for (int i = 0; i < n; i++) {
            outlinks[i] = dis.readInt();
        }
        dis.close();
        return new Graphlet(start, end, n, inlinks, ancestors, outlinks);
    }

    int computeMultilinks() {
	int nnz = 0;
	for(int i = 0; i < size; i++) {
	    nnz = nnz + IntArrays.removeDuplicates(ancestors[i]).length;
	}
	return nnz;
    }

    //////////////////////////////////////////////////////////////////////
    // getters
    //////////////////////////////////////////////////////////////////////
    /**
     * Returns first page of this fragment; fragment includes <code>size</code>
     * consecutive pages
     *
     * @return first page of this fragment
     */
    public int getStart() {
	return start;
    }

    /**
     * Returns first page of next fragment.
     *
     * @return next page of this fragment
     */
    public int getEnd() {
	return end;
    }

    /**
     * Returns number of all pages in the graph.
     *
     * @return number of all pages in the graph.
     */
    public int getN() {
	return n;
    }

    /**
     * Returns number of inbound links for fragment pages.
     *
     * @return number of inbound links for fragment pages.
     */
    public int[] getInlinks() {
	return inlinks;
    }

    /**
     * Returns pages pointing to fragment pages.
     *
     * @return pages pointing to fragment pages.
     */
    public int[][] getAncestors() {
	return ancestors;
    }

    /**
     * Returns number of outbound links for all pages.
     *
     * @return number of outbound links for all pages.
     */
    public int[] getOutlinks() {
	return outlinks;
    }

    /**
     * Returns number of dangling pages.
     *
     * @return number of dangling pages.
     */
    public int getDanglingNum() {
	return danglingNum;
    }

    /**
     * Returns dangling pages.
     *
     * @return dangling pages.
     */
    public int[] getDanglingPages() {
	return danglingPages;
    }

    /**
     * Returns number of fragment pages, i.e. <code>end - start</code>
     *
     * @return number of fragment pages, i.e. <code>end - start</code>
     */
    public int getSize() {
	return size;
    }

    /**
     * Returns number of fragment links.
     *
     * @return number of fragment links.
     */
    public int getLinks() {
	return links;
    }

    /**
     * Returns number of multilinks.
     * This is also known as <code>nnz</code> in sparse matrix representations.
     *
     * @return number of multilinks.
     */
    public int getMultilinks() {
	return multilinks;
    }

    /**
     * Returns percentage of fragment links with respect to <code>size *
     * n</code> (sparsity)
     *
     * @return sparsity
     */
    public double getSparsity() {
	return sparsity;
    }

    /**
     * Prints info for the graph.
     */
    public void info() {
	System.out.print("start = ");
	System.out.println(start);
	System.out.print("end = ");
	System.out.println(end);
	System.out.print("n = ");
	System.out.println(n);
	System.out.print("dangling = ");
	System.out.println(danglingNum);
	System.out.print("links = ");
	System.out.println(links);
	System.out.print("nnz = ");
	System.out.println(multilinks);
	System.out.print("sparsity = ");
	System.out.println(sparsity);
    }


}


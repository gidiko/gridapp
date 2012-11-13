package webmatrix.util;

import java.util.*;
import java.io.*;

/**
 * Utility methods for arrays of ints.
 */
public class IntArrays {

    //////////////////////////////////////////////////////////////////////
    // Methods with broad applicability
    //////////////////////////////////////////////////////////////////////

    /**
     * Returns an array of data contained in given <code>vector</code>
     *
     * @param vector vector containing data.
     * @return array of data.
     */
    public static int[] toArray(Vector<Integer> vector) {
	int size = vector.size();
        int[] result = new int[size];
        Enumeration<Integer> elements = vector.elements();
        int index = 0;
        while (elements.hasMoreElements()) {
            result[index] = elements.nextElement().intValue();
            index++;
        }
	return result;
    }


    /**
     * Returns a shuffling  of <code>data</code>.
     *
     * @param data data to shuffle.
     * @return shuffled data.
     */
    public static int[] shuffle(int[] data) {
	int size = data.length;
	int[] dataShuffled = new int[size];
	System.arraycopy(data, 0, dataShuffled, 0, size);
	for (int i = 0; i < size; i++) {
	    int target = i + (int)(Math.random() * (size - i));
	    int temp = dataShuffled[i];
	    dataShuffled[i] = dataShuffled[target];
	    dataShuffled[target] = temp;
	}
	return dataShuffled;
    }


    /**
     * Permutes entries in <code>data</code> according to given permutation
     * vector <code>map</code>.
     * data[i] <- data[map[i]].
     *
     * @param data data to permute.
     * @param map permutation vector.
     * @return data with permutation applied.
     */
    public static  int[] permute(int[] data, int[] map) {
	int n = map.length;
	int[] dataPermuted = new int[n];
	for (int i = 0; i < n; i++) {
	    dataPermuted[i] = data[map[i]];
	}
	return dataPermuted;
    }

    /**
     * Returns a permutation which when applied to x vector will give y vector.
     *
     * @param x vector to be permuted.
     * @param y vector to which we want to permute to.
     * @return permutation.
     */
    public static int[] permutation(int[] x, int[] y) {
	int size = x.length;
	int[] xranks = new int[size];
	int[] iyranks = new int[size];
	int[] map = new int[size];

	int[] xsorted = new int[size];
	System.arraycopy(x, 0, xsorted, 0, size);
	Arrays.sort(xsorted);
	for (int i = 0; i < size; i++) {
	    int pos = Arrays.binarySearch(xsorted, x[i]);
	    xranks[i] = pos;
	}

	int[] ysorted = new int[size];
	System.arraycopy(y, 0, ysorted, 0, size);
	Arrays.sort(ysorted);
	for (int i = 0; i < size; i++) {
	    int pos = Arrays.binarySearch(ysorted, y[i]);
	    iyranks[pos] = i;
	}
	for (int i = 0; i < size; i++) {
	    map[i] = xranks[iyranks[i]];
	}
	return map;
    }


    /**
     * Normalizes data to unity (sum of its elements set to one)
     * Divides each entry by data sum
     *
     * @param data data to normalize.
     * @return normalized data.
     */
    public static double[] normalize(int[] data) {
	int sum = sum(data);
	return scale(data, 1.0 / sum);
    }


    /**
     * Returns an array of <code>n</code> random numbers within <code>[0,
     * max)</code>
     *
     * @param n  length of the array.
     * @param max maximum random integer (exclusive) drawn.
     * @return array of  random numbers.
     */
    public static int[] random(int n, int max) {
	int[] data = new int[n];
	Random random = new Random();
	for(int i = 0; i < n; i++) {
	    data[i] = random.nextInt(max);
	}
	return data;
    }


    /**
     * Returns a 2D array of <code>m x n</code> random numbers within <code>[0,
     * max)</code>
     *
     * @param m number of rows.
     * @param n number of columns.
     * @param max maximum random integer (exclusive) drawn.
     * @return 2D array of  random numbers.
     */
    public static int[][] random(int m, int n, int max) {
	int[][] data = new int[m][n];
	for(int i = 0; i < m; i++) {
	    data[i] = random(n, max);
	}
	return data;
    }


    /**
     * Removes duplicates.
     *
     * @param data data with possible duplicate entries.
     * @return data with duplicates removed.
     */
    public static int[] removeDuplicates(int[] data) {
	int n = data.length;
	Set set = new HashSet<Integer>();
	for (int i = 0; i < n; i++) {
	    set.add(data[i]);
	}
	Vector<Integer> uniquesVector = new Vector<Integer>();
	uniquesVector.addAll(set);
	int[] uniques = toArray(uniquesVector);
	return uniques;
    }


    /**
     * Removes duplicates from leaf arrays in data.
     *
     * @param data data with possible duplicate entries in leaf arrays.
     * @return data with duplicate entries in leaf arrays removed.
     */

    public static int[][] removeDuplicates(int[][] data) {
	int size = data.length;
	int[][] condensed = new int[size][];
	for (int i = 0; i < size; i++) {
	    int[] leaf = removeDuplicates(data[i]);
	    condensed[i] = leaf;
	}
	return condensed;
    }



    /**
     * Returns a count of the appearances of an element within some data.
     *
     * @param data data searched in.
     * @param element element searched for.
     * @return number of counts.
     */
    public static int count(int data[], int element) {
	int counter = 0;
	int size = data.length;
	for(int i = 0; i < size; i++) {
	    if(data[i] == element) {
		counter++;
	    }
	}
	return counter;
    }


    /**
     * Returns an array of positions of element within some data.
     *
     * @param data data searched in.
     * @param element element searched for.
     * @return array of positions.
     */
    public static int[] position(int data[], int element) {
	int size = data.length;
	int counts = count(data, element);
	int[] index = new int[counts];
	int counter = 0;
	for (int i = 0; i < size; i++) {
	    if(data[i] == element) {
		index[counter] = i;
		counter++;
	    }
	}
	return index;
    }


    /**
     * Returns a range of elements within <code>[0.0, end)</code> with <code>step =
     * 1.0</code> (starting from <code>0.0</code>).
     *
     * @param end upper bound for range.
     * @return range of elements
     */
    public static int[] range(int end) {
	return range(0, end);
    }


    /**
     * Returns a range of elements within <code>[start, end)</code> with <code>step =
     * 1.0</code> (starting from <code>start</code>).
     *
     * @param start lower bound for range.
     * @param end upper bound for range.
     * @return range of elements
     */
    public static int[] range(int start, int end) {
	return range(start, end, 1);
    }


    /**
     * Returns a range of elements within <code>[start, end)</code> with given
     * <code>step</code> (starting from <code>start</code>).
     *
     * @param start lower bound for range.
     * @param end upper bound for range.
     * @param step distance of consecutive elements
     * @return range of elements
     */
    public static int[] range(int start, int end, int step) {
	int num = end - start;
	int steps = (int)Math.ceil(num / step);
	int[] result = new int[steps];
	for (int i = 0; i < steps; i++) {
	    result[i] = start + i * step;
	}
	return result;
    }


    /**
     * Returns a 2D array by splitting <code>data</code> into given number of
     * <code>rows</code>
     *
     * @param data data to split.
     * @param rows number of rows.
     * @return 2D array.
     */
    public static int[][] splitByRows(int[] data, int rows) {
	int size = data.length;
	int columns = size / rows;
	int[][] result = new int[rows][columns];
	int i, j;
	for (int k = 0; k < size ; k++) {
	    i = k / columns;
	    j = k % columns;
	    result[i][j] = data[k];
	}
	return result;
    }


    /**
     * Returns a 2D array by splitting <code>data</code> into given number of
     * <code>columns</code>
     *
     * @param data data to split.
     * @param columns number of columns.
     * @return 2D array.
     */
    public static int[][] splitByColumns(int[] data, int columns) {
	int size = data.length;
	int rows = size / columns;
	int[][] result = new int[rows][columns];
	int i, j;
	for (int k = 0; k < size ; k++) {
	    i = k % rows;
	    j = k / rows;
	    result[i][j] = data[k];
	}
	return result;
    }


    /**
     * Returns a 1D array of data by traversing rectangular array <code>a</code>
     * in row-major order (C-like).
     *
     * @param a rectangular array traversed.
     * @return 1D array.
     */
    public static int[] packByRows(int[][] a) {
	int rows = a.length;
	int columns = a[0].length;
	int size = rows * columns;
	int[] result = new int[size];
	int counter = 0;
	for (int i = 0; i < rows; i++) {
	    for (int j = 0 ; j < columns; j++) {
		result[counter] = a[i][j];
		counter++;
	    }
	}
	return result;
    }


    /**
     * Returns a 1D array of data by traversing rectangular array <code>a</code>
     * in column-major order (Fortran-like).
     *
     * @param a rectangular array traversed.
     * @return 1D array.
     */
    public static int[] packByColumns(int[][] a) {
	int rows = a.length;
	int columns = a[0].length;
	int size = rows * columns;
	int[] result = new int[size];
	int counter = 0;
	for (int j = 0; j < columns; j++) {
	    for (int i = 0 ; i < rows; i++) {
		result[counter] = a[i][j];
		counter++;
	    }
	}
	return result;
    }


    /**
     * Returns a 1D array of data by traversing array <code>a</code>
     * in row-major order (C-like). Note that the array does not have to be rectangular.
     *
     * @param a rectangular array traversed.
     * @return 1D array.
     */
    public static int[] flatten(int[][] a) {
	int rows = a.length;
	int size = 0;
	for (int i = 0; i < rows; i++) {
	    size = size + a[i].length;
	}
	int[] result = new int[size];
	int counter = 0;
	for (int i = 0; i < rows; i++) {
	    int cols = a[i].length;
	    for (int j = 0 ; j < cols; j++) {
		result[counter] = a[i][j];
		counter++;
	    }
	}
	return result;
    }


    /**
     * Returns norm1 of the difference of two 1D data arrays.
     * Norm1 is the sum of absolute values.
     *
     * @param x a data array.
     * @param y the other data array.
     * @return norm1.
     */
    public static int diffNorm1(int[] x, int[] y) {
	int size = x.length;
	int diff = 0;
	for(int i = 0; i < size; i++) {
	    diff = diff + Math.abs(x[i] - y[i]);
	}
	return diff;
    }


    /**
     * Returns norm of the difference of two 1D data arrays.
     * Norm2 is also known as the Euclidean distance: the square root of the sum
     * of squares.
     *
     * @param x a data array.
     * @param y the other data array.
     * @return norm2.
     */
    public static double diffNorm2(int[] x, int[] y) {
	int size = x.length;
	double diff = 0.0;
	for(int i = 0; i < size; i++) {
	    int temp = x[i] - y[i];
	    diff = diff + temp * temp;
	}
	return Math.sqrt(diff);
    }


    /**
     * Returns normInf of the difference of two 1D data arrays.
     * NormInf is the maximum of the absolute values.
     *
     * @param x a data array.
     * @param y the other data array.
     * @return normInf.
     */
    public static int diffNormInf(int[] x, int[] y) {
	int size = x.length;
	int max = Math.abs(x[0] - y[0]);
	for (int i = 1;  i < size; i++) {
	    int abs = Math.abs(x[i] - y[i]);
	    if (abs > max) {
		max = abs;
	    }
	}
	return max;
    }



    /**
     * Returns absolute values of given data array elements.
     *
     * @param data data array.
     * @return absolute values.
     */
    public static int[] abs(int[] data) {
	int size = data.length;
	int[] result = new int[size];
	for (int i = 0; i < size; i++) {
	    result[i] = Math.abs(data[i]);
	}
	return result;
    }


    /**
     * Returns maximum element in <code>data</code>.
     *
     * @param data data.
     * @return maximum.
     */
    public static int max(int[] data) {
	int size = data.length;
	int max = data[0];
	for (int i = 1;  i < size; i++) {
	    if (data[i] > max) {
		max = data[i];
	    }
	}
	return max;
    }


    /**
     * Returns minimum element in <code>data</code>.
     *
     * @param data data.
     * @return minimum.
     */
    public static int min(int[] data) {
	int size = data.length;
	int min = data[0];
	for (int i = 1;  i < size; i++) {
	    if (data[i] < min) {
		min = data[i];
	    }
	}
	return min;
    }


    /**
     * Returns the sum of two 1D data arrays.
     *
     * @param x a data array.
     * @param y the other data array.
     * @return sum.
     */
    public static int[] plus(int[] x, int[] y) {
	int size = x.length;
	int[] result = new int[size];
	for(int i = 0; i < size; i++) {
	    result[i] = x[i] + y[i];
	}
	return result;
    }

    /**
     * Returns the difference of two 1D data arrays.
     *
     * @param x a data array.
     * @param y the data array subtracted.
     * @return difference.
     */
    public static int[] minus(int[] x, int[] y) {
	int size = x.length;
	int[] result = new int[size];
	for(int i = 0; i < size; i++) {
	    result[i] = x[i] - y[i];
	}
	return result;
    }


    /**
     * Returns the element-by-element products of two 1D data arrays.
     *
     * @param x a data array.
     * @param y the other data array.
     * @return element-by-element products.
     */
    public static int[] mult(int[] x, int[] y) {
	int size = x.length;
	int[] result = new int[size];
	for(int i = 0; i < size; i++) {
	    result[i] = x[i] * y[i];
	}
	return result;
    }


    /**
     * Returns the element-by-element quotients of two 1D data arrays.
     *
     * @param x a data array.
     * @param y data array of divisors.
     * @return element-by-element quotients.
     */
    public static double[] div(int[] x, int[] y) {
	int size = x.length;
	double[] result = new double[size];
	for(int i = 0; i < size; i++) {
	    result[i] = x[i] / y[i];
	}
	return result;
    }


    /**
     * Returns <code>data</code> scaled by (each element multiplied by)
     * <code>alpha</code>.
     *
     * @param data data array to scale
     * @param alpha scaling factor.
     * @return scaled data.
     */
    public static double[] scale(int[] data, double alpha) {
	int size = data.length;
	double[] result = new double[size];
	for(int i = 0; i < size; i++) {
	    result[i] = data[i] * alpha;
	}
	return result;
    }

    public static double[] mult(int[] data, double alpha) {
	return scale(data, alpha);
    }

    public static int[] plus(int[] data, int elem) {
	int size = data.length;
	int[] result = new int[size];
	for(int i = 0; i < size; i++) {
	    result[i] = data[i] + elem;
	}
	return result;
    }


    /**
     * Returns only those data contained within closed interval <code>[lower,
     * upper]</code>
     *
     * @param data array to search.
     * @param lower lower bound.
     * @param upper upper bound.
     * @return data contained in closed interval.
     */
    public static int[] filterByBounds(int[] data, int lower, int upper) {
	int size = data.length;
	int counter = 0;
	// how many
	for (int i = 0; i < size; i++) {
	    double product = ((double)(data[i] - lower)) * (data[i] - upper);
	    if (product <= 0) {
		counter++;
	    }
	}
	// store them
	int[] result = new int[counter];
	counter = 0;
	for (int i = 0; i < size; i++) {
	    double product = ((double)(data[i] - lower)) * (data[i] - upper);
	    if (product <= 0) {
		result[counter] = data[i];
		counter++;
	    }
	}
	return result;
    }

    /**
     * Returns only those data NOT contained within closed interval <code>[lower,
     * upper]</code>
     *
     * @param data array to search.
     * @param lower lower bound.
     * @param upper upper bound.
     * @return data NOT contained in closed interval.
     */
    public static int[] filterByOffBounds(int[] data, int lower, int upper) {
	int size = data.length;
	int counter = 0;
	// how many
	for (int i = 0; i < size; i++) {
	    double product = ((double)(data[i] - lower)) * (data[i] - upper);
	    if (product > 0) {
		counter++;
	    }
	}
	// store them
	int[] result = new int[counter];
	counter = 0;
	for (int i = 0; i < size; i++) {
	    double product = ((double)(data[i] - lower)) * (data[i] - upper);
	    if (product > 0) {
		result[counter] = data[i];
		counter++;
	    }
	}
	return result;
    }


    /**
     * Returns the inner product of two 1D data arrays.
     * This is just the sum of its element-by-element products.
     *
     * @param x a data array.
     * @param y the other data array.
     * @return inner product.
     */
    public static int innerMult(int[] x, int[] y) {
	int size = x.length;
	int result = 0;
	for(int i = 0; i < size; i++) {
	    result = result + x[i] * y[i];
	}
	return result;

    }


    /**
     * Returns the outer product of two 1D data arrays.
     * This is a 2D array containing elements of the form <code>a[i,j] = x[i] *
     * y[j]</code>.
     *
     * @param x a data array.
     * @param y the other data array.
     * @return outer product.
     */
    public static int[][] outerMult(int[] x, int[] y) {
	int sizex = x.length;
	int sizey = y.length;
	int[][] result = new int[sizex][sizey];
	for (int i = 0; i < sizex; i++) {
	    for (int j = 0; j < sizey; j++) {
		result[i][j] = x[i] * y[j];
	    }
	}
	return result;
    }


    /**
     * Returns the sum of elements in given 1D data array.
     *
     * @param data data array.
     * @return sum of elements.
     */
    public static int sum(int[] data) {
	int size = data.length;
	int sum = 0;
	for (int i = 0; i < size; i++) {
	    sum = sum + data[i];
	}
	return sum;
    }


    /**
     * Returns a copy of given 1D data array.
     *
     * @param data data array.
     * @return copy.
     */
    public static int[] copy(int[] data) {
	int size = data.length;
	int[] result = new int[size];
	System.arraycopy(data, 0, result, 0, size);
	return result;
    }


    /**
     * Returns a copy of given 2D data array.
     *
     * @param data data array.
     * @return copy.
     */
    public static int[][] copy(int[][] data) {
	int sizex = data.length;
	int[][] result = new int[sizex][];
	for (int i = 0; i < sizex; i++) {
	    int sizey = data[i].length;
	    result[i] = new int[sizey];
	    System.arraycopy(data[i], 0, result[i], 0, sizey);
	}
	return result;
    }


    /**
     * Returns true if given data arrays are exactly the same.
     *
     * @param x a data array.
     * @param y the other data array.
     * @return true if given data arrays are exactly the same.
     */
    public static boolean same(int[] x, int[] y) {
	int sizex = x.length;
	int sizey = y.length;
	if (sizex != sizey){
	    return false;
	}
	for (int i = 0; i < sizex; i++){
	    if(x[i] != y[i]) {
		return false;
	    }
	}
	return true;
    }


    /**
     * Returns true if given data arrays are exactly the same.
     *
     * @param x a data array.
     * @param y the other data array.
     * @return true if given data arrays are exactly the same.
     */
    public static boolean same(int[][] x, int[][] y) {
	int sizex = x.length;
	int sizey = y.length;
	if (sizex != sizey){
	    return false;
	}
	for (int i = 0; i < sizex; i++) {
	    if (!same(x[i], y[i])){
		return false;
	    }
	}
	return true;
    }


    /**
     * Returns an array of row sizes in given data matrix.
     *
     * @param data data matrix
     * @return array of row sizes
     */
    public static int[] sizes(int[][] data) {
	int size = data.length;
	int[] howlong = new int[size];
	for (int i = 0; i < size;  i++) {
	    howlong[i] = data[i].length;
	}
	return howlong;
    }


    /**
     * Returns 1D skeleton array with space allocated for <code>m</code>
     * elements.
     *
     * @return 1D skeleton array.
     */
    public static int[] make(int m) {
	int[] result = new int[m];
	return result;
    }


    /**
     * Returns 2D skeleton array with space allocated for <code>m x n</code>
     * elements.
     *
     * @return 2D skeleton array.
     */
    public static int[][] make(int m, int n) {
	int[][] result = new int[m][n];
	return result;
    }


    /**
     * Assigns <code>value</code> to all data array elements.
     *
     * @param data 1D data array to change.
     * @param value value to assign.
     */
    public static void assign(int[] data, int value) {
	Arrays.fill(data, value);
    }


    /**
     * Assigns <code>value</code> to all data array elements.
     *
     * @param data 2D data array to change.
     * @param value value to assign.
     */
    public static void assign(int[][] data, int value) {
	int size = data.length;
	for (int i = 0; i < size; i++) {
	    Arrays.fill(data[i], value);
	}
    }

    /**
     * Returns 1D array with all its <code>n</code> elements set to
     * <code>value</code>.
     *
     * @param n number of elements.
     * @param value value to assign.
     * @return 1D array.
     */
    public static int[] repeat(int value, int n) {
	int[] result = new int[n];
	Arrays.fill(result, value);
	return result;
    }


    /**
     * Appends array <code>x</code> to <code>data</code> array and returns the
     * increased-size array.
     *
     * @param data the original array.
     * @param x array to append to the original.
     * @return increased-size array.
     */
    public static int[] append(int[] data, int[] x) {
	int size = data.length + x.length;
	int[] result = new int[size];
	System.arraycopy(data, 0, result, 0, data.length);
	System.arraycopy(x, 0, result, data.length, x.length);
	return result;
    }


    /**
     * Returns ranking order of data vector, meaning the order imposed by
     * the values of its elements.
     * ranks[i] contains the integer rank of data[i]. It
     * follows that maximum value in data  is found for i with ranks[i] = 0
     *
     * @param data containing ranking values.
     * @return integer ranks of data.
     */
    public static int[] ranking(int[] data) {
	int size = data.length;
	int[] datasorted = new int[size];
	int[] ranks = new int[size];
	System.arraycopy(data, 0, datasorted, 0, size);
	Arrays.sort(datasorted);
	for(int i = 0; i < size; i++) {
	    int pos = Arrays.binarySearch(datasorted, data[i]);
	    // but sorting was in ascending order, so...
	    ranks[i] = size - pos - 1;
	}
	return ranks;
    }


    public static int[][] repeatLimits(int[] data) {
	int[] bounds = flips(data);
	int bsize = bounds.length;
	int size = bsize + 1;
	int[][] limits = new int[size][2];
	for (int i = 0; i < bsize; i++) {
	    limits[i][1] = bounds[i];
	}
	for (int i = 1; i < size; i++) {
	    limits[i][0] = bounds[i - 1];
	}
	limits[0][0] = 0;
	limits[size - 1][1] = data.length;
	return limits;
    }


    public static int[] flips(int[] data) {
	int size = data.length;

	int[] diffs = gaps(data, 1);
	int dsize = diffs.length;
	int changes = 0;
	for (int i = 0; i < dsize; i++) {
	    if (diffs[i] != 0) {
		changes++;
	    }
	}

	int[] index = new int[changes];
	changes = 0;
	for (int i = 0; i < dsize; i++) {
	    if (diffs[i] != 0.0) {
		index[changes] = i + 1;
		changes++;
	    }
	}
	return index;
    }


    // data[i + gap] - data[i]
    public static int[] gaps(int[] data, int gap) {
	int size = data.length;
	int dsize = size - gap;
	int[] diffs = new int[dsize];
	for (int i = 0; i < dsize; i++) {
	    diffs[i] = data[i + gap] - data[i];
	}
	return diffs;
    }


    /**
     * Returns a permutation which when applied to data vector will sort it in
     * descending order. permutation[i] contains the position of the i-th
     * largest entry in data. It follows that permutation[0] is the index to
     * largest value in data vector.
     *
     * @param data containing ranking values.
     * @return descending order data permutation.
     */
    public static int[] rankingPermutation(int[] data) {
	int size = data.length;
	int[] map = new int[size];
	int[] ranks = ranking(data);
	for (int i = 0; i < size; i++) {
	    map[ranks[i]] = i;
	}
	return map;
    }


    /**
     * Returns perturbation index between two vectors.
     * This is calculated by accumulating terms of the form <code> gap * weight
     * / size </code> where gap is the difference between ranking positions of a
     * node in rankings contained in a and b, weighted by actual rank
     * (i.e. changes in ranking for high rank nodes contribute more than changes
     * in the tail of rankings.
     *
     * @param a vector against which we compare.
     * @param b vector compared to <code>a>/code>.
     * @return perturbation index.
     */
    public static double rankingPerturbation(int[] a, int[] b) {
	int size = a.length;
	int[] map = new int[size];
	int[] aranks = rankingPermutation(a);
	int[] branks = rankingPermutation(b);
	// map[k]: what is the ranking of node k?
	for (int i = 0; i < size; i++) {
	    map[branks[i]] = i;
	}
	// perturbation index
	double pindex = 0.0;
	for (int i = 0; i < size; i++) {
	    int gap;
	    int weight = size - i;
	    int apos = aranks[i];
	    // bpos: the ranking of node apos after.
	    int bpos = map[apos];
	    // i: this is the ranking of node apos before.
	    gap = Math.abs(bpos - i);
	    pindex = pindex + gap * (double)weight / (double)size;
	}
	return pindex;
    }


    //////////////////////////////////////////////////////////////////////
    // Methods with more or less project applicability
    //////////////////////////////////////////////////////////////////////

    /**
     * Returns links produced by reversing given ones.
     * For example given ancestors[][] this method will produce successors[][]
     * and vice versa. Note that ancestors[i] contains nodes pointing to i and
     * successors[i] contains nodes pointed by i.
     *
     * @param links links to reverse
     * @return reversed links
     */
    public static int[][] reverse(int[][] links) {
	int n = links.length;
	int[][] linksReversed = new int[n][];
	int[] counters = new int[n];

	// find the lengths of reversed link entries
	for (int i = 0; i < n; i++) {
	    int connections = links[i].length;
	    for (int j = 0; j < connections; j++) {
		int target = links[i][j];
		counters[target]++;
	    }
	}

	// allocate space for reversed links
	for (int i = 0; i < n; i++) {
	    linksReversed[i] = new int[counters[i]];
	}

	// zero length counters
	// now lengths are built into reversed link entries
	for (int i = 0;  i < n; i++) {
	    counters[i] = 0;
	}

	// fill up reversed link entries
	for (int i = 0; i < n; i++) {
	    int connections = links[i].length;
	    for (int j = 0; j < connections; j++) {
		int target = links[i][j];
		linksReversed[target][counters[target]] = i;
		counters[target]++;
	    }
	}
	return linksReversed;
    }


    /**
     * Returns all links produced by fusing given links with reversed links.
     * For example given either ancestors[][] or successors[][] this method will
     * produce neighbors[][]. Note that ancestors[i] contains nodes pointing to
     * i, successors[i] contains nodes pointed by i, and neighbors[i] contains
     * nodes either pointing to i or pointed by i.
     *
     * @param links links to reverse and fuse.
     * @return fused (neighbor) links.
     */
    public static int[][] symmetrize(int[][] links) {
	int n = links.length;
	int[][] linksSymmetrized = new int[n][];
	int[] counters = new int[n];

	// find the lengths of reversed link entries
	for (int i = 0; i < n; i++) {
	    int connections = links[i].length;
	    for (int j = 0; j < connections; j++) {
		int target = links[i][j];
		counters[target]++;
	    }
	}

	// allocate space for symmetrized links
	for (int i = 0; i < n; i++) {
	    // sum of inlinks and outlinks for a node
	    int size = counters[i] + links[i].length;
	    linksSymmetrized[i] = new int[size];
	}

	// zero reversed length counters
	// now total lengths are built into symmetrized link entries
	for (int i = 0;  i < n; i++) {
	    counters[i] = 0;
	}

	// fill up symmetrized link entries
	for (int i = 0; i < n; i++) {
	    int connections = links[i].length;
	    for (int j = 0; j < connections; j++) {
		int target = links[i][j];
		// i points to target
		linksSymmetrized[target][counters[target]] = i;
		counters[target]++;
		// target point to i
		linksSymmetrized[i][counters[i]] = target;
		counters[i]++;
	    }
	}
	return linksSymmetrized;
    }


    /**
     * Returns	an array of pairs <code>[start, end)</code> arising when
     * breaking range <code>[0, n)</code> into <code>m</code> subranges.
     *
     * @param n length of range.
     * @param m number of subranges.
     * @return array of <code>[start, end)</code> pairs.
     */
    public static int[][] partLimits(int n, int m) {
	return partLimits(n, m, 0);
    }


    /**
     * Returns an array of pairs <code>[start, end)</code> arising when
     * breaking range <code>[base, base + n)</code> into <code>m</code> subranges.
     *
     * @param n length of range.
     * @param m number of subranges.
     * @param base start index of first subrange.
     * @return array of <code>[start, end)</code> pairs.
     */
    public static int[][] partLimits(int n, int m, int base) {
	int partSize = n / m;
	int remaining = n % m;
	int[][] limits = new int[m][2];
	int cursor = base;
	for (int i = 0; i < m - 1; i++) {
	    limits[i][0] = cursor;
	    limits[i][1] = cursor + partSize;
	    cursor = cursor + partSize;
	}
	limits[m - 1][0] = cursor;
	limits[m - 1][1] = cursor + partSize + remaining;
	return limits;
    }


    /**
     * Returns an array of the sizes of subranges arising when
     * breaking range <code>[0, n)</code> into <code>m</code> such subranges.
     *
     * @param n length of range.
     * @param m number of subranges.
     * @return array of <code>[start, end)</code> pairs.
     */
    public static int[] partSizes(int n, int m) {
	int[] sizes = new int[m];
	int[][] partLimits = partLimits(n, m, 0);
	for (int i = 0; i < m; i++) {
	    int start = partLimits[i][0];
	    int end = partLimits[i][1];
	    sizes[i] = end - start;
	}
	return sizes;
    }


    /**
     * Loads data array from binary file.
     * The binary file is supposed to successively contain:
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
     * <td><code>int[m * n]</code></td>
     * <td><code>data</code></td>
     * <td>vector of values</td>
     * </tr>
     * </table>
     * </p>
     * <p>
     * Total number of bytes: <code> 8 + 8 * m * n </code>
     * </p>
     *
     * @param filename  name of file.
     * @return 2D data array.
     * @throws IOException if file cannot be read.
     */
    public static int[][] load(String filename)  throws IOException {
	DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(filename)));
	int m = dis.readInt();
	int n = dis.readInt();
	int num = m * n;
	int[] data = new int[num];
	for (int i = 0; i < num; i++) {
	    data[i] = dis.readInt();
	}
	dis.close();
	int[][] result = splitByRows(data, m);
	return result;
    }


    /**
     * Stores 2D data array to binary file.
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
     * <td><code>int[m * n]</code></td>
     * <td><code>data</code></td>
     * <td>array of values</td>
     * </tr>
     * </table>
     * </p>
     * <p>
     * Total number of bytes: <code> 8 + 8 * m * n </code>
     * </p>
     * @param data 2D data array to be written.
     * @param filename  name of file.
     * @param rowmajor if true, writes 2D data array in row-major order.
     * @throws IOException if file cannot be written.
     */
    public static void dump(int[][] data, String filename, boolean rowmajor) throws IOException {
	DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filename)));
	int m = data.length;
	int n = data[0].length;
	int num = m * n;
	dos.writeInt(m);
	dos.writeInt(n);
	int[] dataToWrite = null;
	if (rowmajor) {
	    dataToWrite = packByRows(data);
	} else {
	    dataToWrite = packByColumns(data);
	}
	for (int i = 0; i < num; i++) {
	    dos.writeInt(dataToWrite[i]);
	}
	dos.close();
    }


    public static void dump(int[][] data, String filename) throws IOException {
	dump(data, filename, true);
    }


    /**
     * Stores 1D data array to binary file.
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
     * <td><code>int[m * n]</code></td>
     * <td><code>data</code></td>
     * <td>vector of values</td>
     * </tr>
     * </table>
     * </p>
     * <p>
     * Total number of bytes: <code> 8 + 8 * m * n </code>
     * </p>
     * One of m, n is 1.
     * @param data 1D data array.
     * @param filename  name of file.
     * @param row if true, writes data array as a row vector (m=1).
     * @throws IOException if file cannot be written.
     */
    public static void dump(int[] data, String filename, boolean row) throws IOException {
	DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filename)));
	int num = data.length;
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
	    dos.writeInt(data[i]);
	}
	dos.close();
    }


    public static void dump(int[] data, String filename) throws IOException {
	dump(data, filename, true);
    }


    /**
     * Returns 1D data array with elements in reverse order
     *
     * @param data data array to reverse.
     * @return reversed data array.
     */
    public static int[] reverse(int[] data) {
	int size = data.length;
	int[] reversed = new int[size];
	for (int i = 0; i < size; i++) {
	    reversed[i] = data[size - i - 1];
	}
	return reversed;
    }

}


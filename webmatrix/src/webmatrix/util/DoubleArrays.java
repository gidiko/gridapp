package webmatrix.util;

import java.util.*;
import java.io.*;


/**
 * Utility methods for arrays of doubles.
 */
public class DoubleArrays {

	//////////////////////////////////////////////////////////////////////
	// Methods with broad applicability
	//////////////////////////////////////////////////////////////////////

	/**
	 * Returns an array of data contained in given <code>vector</code>
	 *
	 * @param vector vector containing data.
	 * @return array of data.
	 */
	public static double[] toArray(Vector<Double> vector) {
		int size = vector.size();
        double[] result = new double[size];
        Enumeration<Double> elements = vector.elements();
        int index = 0;
        while (elements.hasMoreElements()) {
            result[index] = elements.nextElement().doubleValue();
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
	public static double[] shuffle(double[] data) {
		int size = data.length;
		double[] dataShuffled = new double[size];
		System.arraycopy(data, 0, dataShuffled, 0, size);
		for (int i = 0; i < size; i++) {
			int target = i + (int)(Math.random() * (size - i));
			double temp = dataShuffled[i];
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
	public static  double[] permute(double[] data, int[] map) {
		int n = map.length;
		double[] dataPermuted = new double[n];
		for (int i = 0; i < n; i++) {
			dataPermuted[i] = data[map[i]];
		}
		return dataPermuted;
	}


	/**
	 * Normalizes data to unity (sum of its elements set to one)
	 * Divides each entry by data sum
	 *
	 * @param data data to normalize.
	 * @return normalized data.
	 */
	public static double[] normalize(double[] data) {
		double sum = sum(data);
		return scale(data, 1.0 / sum);
	}


	/**
	 * Returns an array of <code>n</code> random numbers.
	 *
	 * @param n  length of the array.
	 * @return array of  random numbers.
	 */
	public static double[] random(int n) {
		double[] data = new double[n];
		for(int i = 0; i < n; i++) {
			data[i] = Math.random();
		}
		return data;
    }


	/**
	 * Returns a 2D array of <code>m x n</code> random numbers.
	 *
	 * @param m number of rows.
	 * @param n number of columns.
	 * @return 2D array of  random numbers.
	 */
	public static double[][] random(int m, int n) {
		double[][] data = new double[m][n];
		for(int i = 0; i < m; i++) {
			data[i] = random(n);
		}
		return data;
    }


	/**
	 * Removes duplicates.
	 *
	 * @param data data with possible duplicate entries.
	 * @return data with duplicates removed.
	 */
	public static double[] removeDuplicates(double[] data) {
		int n = data.length;
		Set set = new HashSet<Double>();
		for (int i = 0; i < n; i++) {
			set.add(data[i]);
		}
		Vector<Double> uniquesVector = new Vector<Double>();
		uniquesVector.addAll(set);
		double[] uniques = toArray(uniquesVector);
		return uniques;
	}


    /**
     * Removes duplicates from leaf arrays in data.
     *
     * @param data data with possible duplicate entries in leaf arrays.
     * @return data with duplicate entries in leaf arrays removed.
     */

    public static double[][] removeDuplicates(double[][] data) {
	int size = data.length;
	double[][] condensed = new double[size][];
	for (int i = 0; i < size; i++) {
	    double[] leaf = removeDuplicates(data[i]);
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
	public static int count(double data[], double element) {
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
	public static int[] position(double data[], double element) {
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
	public static double[] range(double end) {
		return range(0.0, end);
	}


	/**
	 * Returns a range of elements within <code>[start, end)</code> with <code>step =
	 * 1.0</code> (starting from <code>start</code>).
	 *
	 * @param start lower bound for range.
	 * @param end upper bound for range.
	 * @return range of elements
	 */
	public static double[] range(double start, double end) {
		return range(start, end, 1.0);
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
	public static double[] range(double start, double end, double step) {
		double num = end - start;
		int steps = (int)Math.ceil(num / step);
		double[] result = new double[steps];
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
	public static double[][] splitByRows(double[] data, int rows) {
		int size = data.length;
		int columns = size / rows;
		double[][] result = new double[rows][columns];
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
	public static double[][] splitByColumns(double[] data, int columns) {
		int size = data.length;
		int rows = size / columns;
		double[][] result = new double[rows][columns];
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
	public static double[] packByRows(double[][] a) {
		int rows = a.length;
		int columns = a[0].length;
		int size = rows * columns;
		double[] result = new double[size];
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
	public static double[] packByColumns(double[][] a) {
		int rows = a.length;
		int columns = a[0].length;
		int size = rows * columns;
		double[] result = new double[size];
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
	public static double[] flatten(double[][] a) {
		int rows = a.length;
		int size = 0;
		for (int i = 0; i < rows; i++) {
			size = size + a[i].length;
		}
		double[] result = new double[size];
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
	public static double diffNorm1(double[] x, double[] y) {
		int size = x.length;
		double diff = 0.0;
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
	public static double diffNorm2(double[] x, double[] y) {
		int size = x.length;
		double diff = 0.0;
		for(int i = 0; i < size; i++) {
			double temp = x[i] - y[i];
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
	public static double diffNormInf(double[] x, double[] y) {
		int size = x.length;
		double max = Math.abs(x[0] - y[0]);
		for (int i = 1;  i < size; i++) {
			double abs = Math.abs(x[i] - y[i]);
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
	public static double[] abs(double[] data) {
		int size = data.length;
		double[] result = new double[size];
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
	public static double max(double[] data) {
		int size = data.length;
		double max = data[0];
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
	public static double min(double[] data) {
		int size = data.length;
		double min = data[0];
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
	public static double[] plus(double[] x, double[] y) {
		int size = x.length;
		double[] result = new double[size];
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
	public static double[] minus(double[] x, double[] y) {
		int size = x.length;
		double[] result = new double[size];
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
	public static double[] mult(double[] x, double[] y) {
		int size = x.length;
		double[] result = new double[size];
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
	public static double[] div(double[] x, double[] y) {
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
	public static double[] scale(double[] data, double alpha) {
		int size = data.length;
		double[] result = new double[size];
		for(int i = 0; i < size; i++) {
			result[i] = data[i] * alpha;
		}
		return result;
	}

	public static double[] mult(double[] data, double alpha) {
		return scale(data, alpha);
	}

	public static double[] plus(double[] data, double elem) {
		int size = data.length;
		double[] result = new double[size];
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
	public static double[] filterByBounds(double[] data, double lower, double upper) {
		int size = data.length;
		int counter = 0;
		// how many
		for (int i = 0; i < size; i++) {
			double product = (data[i] - lower) * (data[i] - upper);
			if (product <= 0) {
				counter++;
			}
		}
		// store them
		double[] result = new double[counter];
		counter = 0;
		for (int i = 0; i < size; i++) {
			double product = (data[i] - lower) * (data[i] - upper);
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
	public static double[] filterByOffBounds(double[] data, double lower, double upper) {
		int size = data.length;
		int counter = 0;
		// how many
		for (int i = 0; i < size; i++) {
			double product = (data[i] - lower) * (data[i] - upper);
			if (product > 0) {
				counter++;
			}
		}
		// store them
		double[] result = new double[counter];
		counter = 0;
		for (int i = 0; i < size; i++) {
			double product = (data[i] - lower) * (data[i] - upper);
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
	public static double innerMult(double[] x, double[] y) {
		int size = x.length;
		double result = 0.0;
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
	public static double[][] outerMult(double[] x, double[] y) {
		int sizex = x.length;
		int sizey = y.length;
		double[][] result = new double[sizex][sizey];
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
	public static double sum(double[] data) {
		int size = data.length;
		double sum = 0.0;
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
	public static double[] copy(double[] data) {
		int size = data.length;
		double[] result = new double[size];
		System.arraycopy(data, 0, result, 0, size);
		return result;
	}

	/**
	 * Returns a copy of given 2D data array.
	 *
	 * @param data data array.
	 * @return copy.
	 */
	public static double[][] copy(double[][] data) {
		int sizex = data.length;
		double[][] result = new double[sizex][];
		for (int i = 0; i < sizex; i++) {
			int sizey = data[i].length;
			result[i] = new double[sizey];
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
	public static boolean same(double[] x, double[] y) {
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
	public static boolean same(double[][] x, double[][] y) {
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
	public static int[] sizes(double[][] data) {
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
	public static double[] make(int m) {
		double[] result = new double[m];
		return result;
	}


	/**
	 * Returns 2D skeleton array with space allocated for <code>m x n</code>
	 * elements.
	 *
	 * @return 2D skeleton array.
	 */
	public static double[][] make(int m, int n) {
		double[][] result = new double[m][n];
		return result;
	}


	/**
	 * Assigns <code>value</code> to all data array elements.
	 *
	 * @param data 1D data array to change.
	 * @param value value to assign.
	*/
	public static void assign(double[] data, double value) {
		Arrays.fill(data, value);
	}


	/**
	 * Assigns <code>value</code> to all data array elements.
	 *
	 * @param data 2D data array to change.
	 * @param value value to assign.
	*/
	public static void assign(double[][] data, double value) {
		int size = data.length;
		for (int i = 0; i < size; i++) {
			Arrays.fill(data[i], value);
		}
	}

	/**
	 * Return 1D array with all its <code>n</code> elements set to
	 * <code>value</code>.
	 *
	 * @param n number of elements.
	 * @param value value to assign.
	 * @return 1D array.
	*/
	public static double[] repeat(double value, int n) {
		double[] result = new double[n];
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
	public static double[] append(double[] data, double[] x) {
		int size = data.length + x.length;
		double[] result = new double[size];
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

    public static int[] ranking(double[] data) {
		int size = data.length;
		double[] datasorted = new double[size];
		int[] ranks = new int[size];
		System.arraycopy(data, 0, datasorted, 0, size);
		Arrays.sort(datasorted);

		int[] starts = new int[size];
		int[] ends = new int[size];
		int[] filled = IntArrays.repeat(0, size);

		int[] changes = flips(datasorted);
		System.arraycopy(changes, 0, starts, 1, size - 1);
		starts[0] = 0;
		System.arraycopy(changes, 0, ends, 0, size - 1);
		ends[size - 1] = size;
		for(int i = 0; i < size; i++) {
			// XXX the ambiguity here is problem source for rankingPermutation()
			int spos = Arrays.binarySearch(datasorted, data[i]);
			int pos = 0;
			if (filled[spos] == 1) {
				int start = starts[spos];
				int end = ends[spos];
				for (int j = start; j < end; j++) {
					if (filled[j] == 0) {
						filled[j] = 1;
						pos = j;
						break;
					}
				}
			} else {
				filled[spos] = 1;
				pos = spos;
			}
			ranks[i] = pos;
		}
		return IntArrays.reverse(ranks);
	}


	public static int[][] repeatLimits(double[] data) {
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


	public static int[] flips(double[] data) {
		int size = data.length;

		double[] diffs = gaps(data, 1);
		int dsize = diffs.length;
		int changes = 0;
		for (int i = 0; i < dsize; i++) {
			if (diffs[i] != 0.0) {
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
	public static double[] gaps(double[] data, int gap) {
		int size = data.length;
		int dsize = size - gap;
		double[] diffs = new double[dsize];
		for (int i = 0; i < dsize; i++) {
			diffs[i] = data[i + gap] - data[i];
		}
		return diffs;
	}


	///
    public static int[] rankingOld(double[] data) {
		int size = data.length;
		double[] datasorted = new double[size];
		int[] ranks = new int[size];
		System.arraycopy(data, 0, datasorted, 0, size);
		Arrays.sort(datasorted);

		double[] uniques = removeDuplicates(data);
		Arrays.sort(uniques);
		int usize = uniques.length;
		int counter = 0;
		int[] starts = new int[size];
		int[] ends = new int[size];
		for (int i = 0;  i < usize; i++) {
			double value = uniques[i];
			int  start = counter;
			int localcounter = 0;

			while((counter < size) && (datasorted[counter] == value)) {
				starts[counter] = start;
				counter++;
			}
			for (int j = start; j < counter; j++) {
				ends[j] = counter;
			}
		}

		int[] filled = new int[size];
		for(int i = 0; i < size; i++) {
			// XXX the ambiguity here is problem source for rankingPermutation()
			int pos = Arrays.binarySearch(datasorted, data[i]);
			// but sorting was in ascending order, so...
			if  (filled[pos] == 0) {
				ranks[i] = pos;
				filled[pos] = 1;
			} else {
				int start = starts[i];
				int end = ends[i];
				for (int j = start; j < end; j++) {
					if (filled[j] == 0){
						ranks[i] = j;
						filled[j] = 1;
						break;
					}
				}
			}
		}
		return IntArrays.reverse(ranks);
	}





	/**
	 * Returns a permutation which when applied to x vector will give y vector.
	 *
	 * @param x vector to be permuted.
	 * @param y vector to which we want to permute to.
	 * @return permutation.
	 */
	public static int[] permutation(double[] x, double[] y) {
		int size = x.length;
		int[] xranks = new int[size];
		int[] iyranks = new int[size];
		int[] map = new int[size];

		double[] xsorted = new double[size];
		System.arraycopy(x, 0, xsorted, 0, size);
		Arrays.sort(xsorted);
		for (int i = 0; i < size; i++) {
			int pos = Arrays.binarySearch(xsorted, x[i]);
			xranks[i] = pos;
		}

		double[] ysorted = new double[size];
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
	 * Returns a permutation which when applied to data vector will sort it in
	 * descending order. permutation[i] contains the position of the i-th
	 * largest entry in data. It follows that permutation[0] is the index to
	 * largest value in data vector.
	 *
	 * @param data containing ranking values.
	 * @return descending order data permutation.
	 */
	public static int[] rankingPermutation(double[] data) {
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
	public static double rankingPerturbation(double[] a, double[] b) {
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
	 * Returns a number which is similar in spirit to greatest common divisor
	 * for a set of floating point numbers.
	 *
	 * @param data floating point numbers.
	 * @return gcd of these floating point numbers.
	 */
	public static double gcd(double[] data) {
		double THRESHOLD = 1.0e-6;
		int size = data.length;
		double unit = 0.0;
		if (size == 0) {
			unit = data[0];
			return unit;
		} else {
			double min = data[0];
			for (int i = 1; i < size; i++) {
				min = Math.min(min, data[i]);
			}
			int k = 0;
			int i = 0;
			double links;
			double absRemainder;
			while (k != size) {
				i++;
				unit = min / i;
				k = 0;
				while (k < size) {
					absRemainder = Math.abs(Math.IEEEremainder(data[k], unit));
					if (absRemainder < THRESHOLD) {
						k++;
					} else {
						break;
					}
				}
			}
		}
		return unit;
	}


	/**
	 * Returns an array of minimum integers with ratios equal to the respective ratios of
	 * numbers in given data.
	 *
	 * @param data floating point numbers.
	 * @return array of minimum integers.
	 */
	public static int[] gcdMultipliers(double[] data) {
		int size = data.length;
		int[] multipliers = new int[size];

		double unit = gcd(data);
		for (int i = 0; i < size; i++) {
			multipliers[i] = (int)Math.round(data[i] / unit);
		}
		return multipliers;
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
	 * <td><code>double[m * n]</code></td>
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
	public static double[][] load(String filename)  throws IOException {
		DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(filename)));
		int m = dis.readInt();
		int n = dis.readInt();
		int num = m * n;
		double[] data = new double[num];
		for (int i = 0; i < num; i++) {
			data[i] = dis.readDouble();
		}
		dis.close();
		double[][] result = splitByRows(data, m);
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
	 * <td><code>double[m * n]</code></td>
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
	public static void dump(double[][] data, String filename, boolean rowmajor) throws IOException {
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filename)));
		int m = data.length;
		int n = data[0].length;
		int num = m * n;
		dos.writeInt(m);
		dos.writeInt(n);
		double[] dataToWrite = null;
		if (rowmajor) {
			dataToWrite = packByRows(data);
		} else {
			dataToWrite = packByColumns(data);
		}
		for (int i = 0; i < num; i++) {
			dos.writeDouble(dataToWrite[i]);
		}
		dos.close();
	}


	public static void dump(double[][] data, String filename) throws IOException {
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
	 * <td><code>double[m * n]</code></td>
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
	public static void dump(double[] data, String filename, boolean row) throws IOException {
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
			dos.writeDouble(data[i]);
		}
		dos.close();
	}


	public static void dump(double[] data, String filename) throws IOException {
		dump(data, filename, true);
	}



	/**
	 * Returns 1D data array with elements in reverse order
	 *
	 * @param data data array to reverse.
	 * @return reversed data array.
	 */
	public static double[] reverse(double[] data) {
		int size = data.length;
		double[] reversed = new double[size];
		for (int i = 0; i < size; i++) {
			reversed[i] = data[size - i - 1];
		}
		return reversed;
	}

	/**
	 * Returns number of smallest elements in data with some not over the value
	 * of nth data element (put in descending order).
	 *
	 * @param data data array to inspect.
	 * @param n nth largest element.
	 * @return number of tail elements.
	 */
	public static int nthWeakerTail(double[] data, int n) {
		int size = data.length;
		// sort first in any case
		double[] dataSorted = new double[size];
		System.arraycopy(data, 0, dataSorted, 0, size);
		Arrays.sort(dataSorted);
		// larger values first
		dataSorted = reverse(dataSorted);
		double limit = data[n];
		double sum = 0.0;
		int num = 0;
		while (sum < limit) {
			sum = sum + dataSorted[size - num - 1];
			num++;
		}
		return num;
	}


	/**
	 * Zeros data elements indexed by <code>index</code> and distributes their
	 * values uniformly to rest data. Sum is preserved.
	 *
	 * @param data data array.
	 * @param index index array of elements to zero.
	 * @return data with zerod positions.
	 */
	public static double[] neutral(double[] data, int[] index) {
		int size = data.length;
		int isize = index.length;
		double[] neutrals = new double[size];
		double sum = 0.0;
		for (int i = 0; i < isize; i++) {
			sum = sum + data[index[i]];
		}
		// actives
		int asize = size - isize;
		double share = sum / asize;

		// sentinel -> +1
		int[] indexSorted = new int[isize + 1];
		System.arraycopy(index, 0, indexSorted, 0, isize);
		indexSorted[isize] = size;
		Arrays.sort(indexSorted);
		int counter = 0;
		for (int i = 0; i < size; i++) {
			if (i == indexSorted[counter]) {
				neutrals[i] = 0.0;
				counter++;
			} else {
				neutrals[i] = data[i] + share;
			}
		}
		return neutrals;
	}






}


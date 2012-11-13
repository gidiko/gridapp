package webmatrix.util;

import no.uib.cipr.matrix.*;
import no.uib.cipr.matrix.sparse.*;



public class PageRanker {
    static double ALPHA = 0.85;
    static int MAX_STEPS = 100;
    static double ERROR_THRESHOLD = 1.0e-4;
    Matrix matrix;
    Vector initial;
    double alpha;
    Vector personal;
    
    int size;
    Vector previous;
    Vector current;
    Vector dangling;
    Vector ones;
    int iterations = 0;
    double error = ERROR_THRESHOLD + 1.0;
    java.util.Vector<Double> errors = new java.util.Vector<Double>();
    private Vector personalTerm;
    
    // Constructors
    public PageRanker(Matrix matrix) {
	this(matrix, MTJMatrices.newUniformVector(matrix.numColumns()));
    }
        
    public PageRanker(Matrix matrix, Vector initial) {
	this(matrix, initial, ALPHA);
    }
    
    public PageRanker(Matrix matrix, Vector initial, double alpha) {
	this(matrix, initial, alpha, MTJMatrices.newUniformVector(matrix.numColumns()));
    }

    public PageRanker(Matrix matrix, Vector initial, double alpha, Vector personal) {
	size = matrix.numColumns();
	dangling = MTJMatrices.computeDanglingVector(matrix);
	personalTerm = personal.copy().scale(1.0 - alpha);
	
	ones = new DenseVector(DoubleArrays.repeat(1.0, size));
	this.matrix = matrix;
	this.initial = initial.copy();
	this.alpha = alpha;
	this.personal = personal.copy();
	current = initial.copy();
    }
    
    
    public void step() {
	previous = current;
	
	double factor = alpha / size * dangling.dot(previous);
	Vector danglingTerm = ones.copy().scale(factor);
	Vector othersTerm = danglingTerm.copy().add(personalTerm);
	current = matrix.multAdd(alpha, previous, othersTerm);  
	
	iterations++;
	
	updateErrors();
	
    }
    
    public void stepUntilErrorThreshold() {
	while (error > ERROR_THRESHOLD) {
	    step();
	}
    }
    
    
    
    private double computeError() {
	Vector diff = current.copy().add(-1.0, previous);
	return diff.norm(Vector.Norm.One);
    }
    
    
    private void updateErrors() {
	error = computeError();
	errors.add(new Double(error));
    }
    
    
    public Vector getCurrent() {
	return current;
    }
    
    
}

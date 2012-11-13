package webmatrix.util;

import no.uib.cipr.matrix.*;
import no.uib.cipr.matrix.sparse.*;



public class MultiDampingRanker {
    static double ALPHA = 0.85;
    static int MAX_STEPS = 100;
    static double ERROR_THRESHOLD = 1.0e-4;
    Matrix matrix;
    Vector initial;
    static double[] alphas = {
	0.45945945946, 0.61127308066, 0.68618836543, 0.73035874084, 0.75917173742,
	0.77922440145, 0.79381711832, 0.80478486703, 0.81322971051, 0.81985374062,
	0.82512555111, 0.82936988128, 0.83281884829, 0.83564269606, 0.83796902403,
	0.83989524054, 0.84149687648, 0.84283328448, 0.84395163869, 0.84488980121,
	0.84567841519, 0.84634245928, 0.84690241992, 0.84737518750, 0.84777475023,
	0.84811273733, 0.84839884852, 0.84864119690, 0.84884658463, 0.84902072644,
	0.84916843181, 0.84929375433, 0.84940011484, 0.84949040332, 0.84956706344,
	0.84963216318, 0.84968745367, 0.84973441862, 0.84977431577, 0.84980821169,
	0.84983701119, 0.84986148209, 0.84988227608, 0.84989994645, 0.84991496299,
	0.84992772469, 0.84993857042, 0.84994778807, 0.84995562218, 0.84996228052,
	0.84996793965, 0.84997274958, 0.84997683777, 0.84998031256, 0.84998326601,
	0.84998577635, 0.84998791007, 0.84998972368, 0.84999126522, 0.84999257550,
	0.84999368922, 0.84999463587, 0.84999544052, 0.84999612446, 0.84999670580,
	0.84999719994, 0.84999761996, 0.84999797697, 0.84999828043, 0.84999853836,
	0.84999875761, 0.84999894397, 0.84999910238, 0.84999923702, 0.84999935147,
	0.84999944875, 0.84999953144, 0.84999960172, 0.84999966146, 0.84999971224,
	0.84999975541, 0.84999979210, 0.84999982328, 0.84999984979, 0.84999987232,
	0.84999989147, 0.84999990775, 0.84999992159, 0.84999993335, 0.84999994335,
	0.84999995185, 0.84999995907, 0.84999996521, 0.84999997043, 0.84999997486,
	0.84999997863, 0.84999998184, 0.84999998456, 0.84999998688
    };

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
    public MultiDampingRanker(Matrix matrix) {
	this(matrix, MTJMatrices.newUniformVector(matrix.numColumns()));
    }

    public MultiDampingRanker(Matrix matrix, Vector initial) {
	this(matrix, initial, MTJMatrices.newUniformVector(matrix.numColumns()));
    }

    public MultiDampingRanker(Matrix matrix, Vector initial, Vector personal) {
	size = matrix.numColumns();
	dangling = MTJMatrices.computeDanglingVector(matrix);


	ones = new DenseVector(DoubleArrays.repeat(1.0, size));
	this.matrix = matrix;
	this.initial = initial.copy();
	// this.alpha = alpha;
	this.personal = personal.copy();
	current = initial.copy();
    }


    public void step() {
	previous = current;

	double alpha = alphas[iterations];
	personalTerm = personal.copy().scale(1.0 - alpha);
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

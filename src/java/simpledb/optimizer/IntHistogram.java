package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private final int[] buckets;
    private final int min;
    private final int max;
    private final double width;
    private int ntups;


    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param bucketsNum The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int bucketsNum, int min, int max) {
    	// some code goes here
        this.buckets = new int[bucketsNum];
        this.min = min;
        this.max = max;
        this.width = (max - min + 1.0) / bucketsNum;
        this.ntups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if (v < min || v > max) {
            throw new IllegalArgumentException("value out of range");
        }
        int idx = (int)((v - min) / width);
        buckets[idx]++;
        ntups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        int idx = -1;
        switch (op) {
            case LESS_THAN:
                if (v <= this.min) return 0.0;
                if (v > this.max) return 1.0;
                idx = (int)((v - this.min) / this.width);
                double cnt = 0.0;
                for (int i = 0; i < idx; i++) {
                    cnt += this.buckets[i];
                }
                cnt += this.buckets[idx] * ((v - (this.min + this.width * idx)) / this.width);
                return cnt / this.ntups;
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);
            case EQUALS:
                if (v < this.min) return 0.0;
                if (v > this.max) return 0.0;
                idx = (int)((v - this.min) / this.width);
                return this.buckets[idx] / (this.width * ntups);
            case NOT_EQUALS:
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
            case GREATER_THAN_OR_EQ:
                return 1 - estimateSelectivity(Predicate.Op.LESS_THAN, v);
            case GREATER_THAN:
                return 1 - estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v);
            default:
                throw new IllegalArgumentException(String.format("unimplemented op %s", op));
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        // todo
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return String.format("IntHistogram: min = %d max = %d buckets = %s", this.min, this.max,
                Arrays.toString(this.buckets));
    }
}

package simpledb;
import java.lang.Math;
import java.util.Arrays;

import org.graalvm.compiler.asm.amd64.AMD64Assembler.VexFloatCompareOp.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int[] hist;
    private double width;
    private int minVal;
    private int nTups;


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
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        width = ((double) (max - min + 1))/buckets;
        if (width < 1) {
            buckets = max-min;  
            // if we get bigger buckets value but max and min of, say, 0 and 1, we only need max - min buckets
        }
        width = Math.max(1, width); // can't have width <1 bc we are working with ints

        hist = new int[buckets];
        minVal = min;
        nTups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int b = getBucket(v);
        hist[b]++;
        nTups++;
    }

    private int getBucket(int v) {
        int b = (int)((v - minVal) / width); // casting as int gets floor value
        b = Math.min(b, hist.length - 1); // if max value is entered, get last bucket
        return b;
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
        int b = getBucket(v);
        double sel = -1;  // if this is returned, we got a problem

        if (op == Predicate.Op.EQUALS || op == Predicate.Op.LIKE || op == Predicate.Op.NOT_EQUALS) {
            sel = (hist[b] / width) / nTups;
            if (op == Predicate.Op.NOT_EQUALS) {
                return 1 - sel;
            }
            else return sel;
        }
        else if (op == Predicate.Op.GREATER_THAN || op == Predicate.Op.GREATER_THAN_OR_EQ) {
            double b_f = b_fraction(b);
            double b_part = (b_right(b) - v) / width;
            if (op == Predicate.Op.GREATER_THAN) {
                b_part = (b_right(b) - v + 1) / width;
                // so as not to include values for v
            }

            sel = b_f * b_part;
            for (int i = b + 1; i < hist.length; i++) {
                sel += (double)hist[i] / nTups;
            }
            return sel;
        }
        else if (op == Predicate.Op.LESS_THAN || op == Predicate.Op.LESS_THAN_OR_EQ) {
            double b_f = b_fraction(b);
            double b_part = (b_left(b) - v) / width;
            if (op == Predicate.Op.LESS_THAN) {
                b_part = (b_left(b) - v + 1) / width;
                // so as not to include values for v
            }

            sel = b_f * b_part;
            for (int i = 0; i < b; i++) {
                sel += (double)hist[i] / nTups;
            }
            return sel;
        }


    	// some code goes here
        return -1.0;
    }

    private double b_left(int b) {
        return (minVal + (b * width));
        // gets left edge
    }

    private double b_right(int b) {
        return (minVal + ((b+1) * width));
        // gets left edge of right bucket
    }

    private double b_fraction(int b) {
        return (double)hist[b] / nTups;
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
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return Arrays.toString(hist);
    }
}

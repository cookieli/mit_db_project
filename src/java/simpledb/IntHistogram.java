package simpledb;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	public int min;
	public int max;
	public int nBuckets;
	public int bucketSize;
	public int[] hist;
	public int[] hist_sum;
	public boolean isSumEvaluated;
	public int nTups;
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
    	// some code goes here
    	this.max = max;
    	this.min = min;
    	this.nBuckets = buckets;
    	this.bucketSize = (int) Math.ceil((1.0*max - 1.0*min)/((double)buckets));
    	if(this.bucketSize == 0)  this.bucketSize = 1;
    	this.hist = new int[this.nBuckets];
    	this.hist_sum = new int[this.nBuckets+ 1];
    	this.isSumEvaluated = false;
    	this.nTups = 0;
    }
    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    
    public int computeOffset(int v) {
    	if(v > max || v < min)
    		return -1;
    	else if (v == max)
    		return nBuckets - 1;
    	else if ((v-min)%bucketSize == 0) {
    		int n = (v - min)/bucketSize;
    		return n;
    	}else {
    		int n = (int) Math.ceil((1.0*v - 1.0*min)/(double)bucketSize);
    		return n -1;
    	}
    }
    public void addValue(int v) {
    	int n = computeOffset(v);
    	if(n == -1) return;
    	hist[n]++;
    	nTups++;
    }
    //i need a sum function to compute the nth hist sum from first hist
    public void evaluateSum() {
    	hist_sum[0] = 0;
    	for(int i = 1; i < hist_sum.length; i++) {
    		for(int j = 0; j < i; j++) {
    			hist_sum[i]+= hist[j];
    		}
    	}
    	isSumEvaluated = true;
    }
    
    public int sum(int i) {
    	if(!isSumEvaluated)
    		evaluateSum();
    	return hist_sum[i];
    }
    
    public int getSumFromBucket(int i, int j) {
    	if(!isSumEvaluated)
    		evaluateSum();
    	//System.out.println("hist_sum j: "+hist_sum[j]);
    	//System.out.println("hist_sum i: "+hist_sum[i]);
    	return hist_sum[j] - hist_sum[i];
    }
    
    public int getSum(int i, int j) {
    	return 0;
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
    	int offset = computeOffset(v);
    	double selectivity = 0.0;
    	// some code goes here
    	switch(op) {
    	case EQUALS:
    		if(offset == -1)
    			return 0.0;
    		//System.out.println((1.0*hist[offset]/1.0*bucketSize)/nTups);
    		return (1.0*hist[offset]/(1.0*bucketSize))/nTups;
    		
    	case NOT_EQUALS:
    		if(offset == -1)
    			return 1.0;
    		return 1 - (1.0*hist[offset]/(1.0*bucketSize))/nTups;
    	case GREATER_THAN:
    	case GREATER_THAN_OR_EQ:
    		if(offset != -1) {
    			//System.out.println("offset " + offset);
    			int rightBound = min + (offset+1) * bucketSize;
    			double bucket_part = (double)(rightBound - v)/bucketSize;
    			double bucket_frac = (double)(hist[offset]/nTups);
    			selectivity = bucket_part*bucket_frac;
    			//System.out.println("selectivity: "+ selectivity);
    			if(op == Predicate.Op.GREATER_THAN_OR_EQ) {
    				if(selectivity != 0.0)
    					selectivity += (double)(hist[offset]/bucketSize)/nTups;
    				else
    					selectivity += (1.0*hist[offset]/(1.0*bucketSize))/nTups;
    				//System.out.println("selectivy: "+selectivity);
    			}
    			//System.out.println("selectivy: "+selectivity);
    			//System.out.println("offset "+offset);
    			//System.out.println("sum "+ (double)getSumFromBucket(offset+1, hist_sum.length -1));
    			selectivity += (double)getSumFromBucket(offset+1, hist_sum.length -1)/nTups;
    			//System.out.println("selectivy: "+selectivity);
    		} else {
    			//System.out.println("it's in this");
    			selectivity = v > max ? 0.0:1.0;
    		}
    		break;
    	case LESS_THAN:
    	case LESS_THAN_OR_EQ:
    		if(offset != -1) {
    			int leftBound = min + offset*bucketSize;
    			double buket_part = (double)(v - leftBound)/bucketSize;
    			double bucket_frac = (double)(hist[offset]/nTups);
    			selectivity = bucket_frac*bucket_frac;
    			if(op == Predicate.Op.LESS_THAN_OR_EQ) {
    				selectivity += (double)(hist[offset]/bucketSize)/nTups;
    			}
    			selectivity += (double)getSumFromBucket(0, offset)/nTups;
    		} else {
    			selectivity = v < min ? 0.0:1.0;
    		}
    		
    	}
        return selectivity;
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
    	StringBuilder sb = new StringBuilder();
    	sb.append("min: ");
    	sb.append(min);
    	sb.append("\tmax: ");
    	sb.append(max);
    	sb.append("\tnBuckets: ");
    	sb.append(nBuckets);
    	sb.append("\tbucketSize: ");
    	sb.append(bucketSize);
    	sb.append("\nhist: ");
    	sb.append(Arrays.toString(hist));
    	sb.append("\nhist_sum: ");
    	sb.append(Arrays.toString(hist_sum));
        return sb.toString();
    }
    
    public static void main(String[] args) {
    	System.out.println(3/2);
    }
}

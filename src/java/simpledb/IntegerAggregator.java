package simpledb;

import java.util.HashMap;
import java.util.LinkedList;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Aggregator.Op op;
    //private ArrayList<Tuple> results;
    private TupleDesc td;
    private HashMap<Field, Tuple> fieldWithTuple;
    private HashMap<Field, MyOperation> fieldWithValue;
    
    private class MyOperation {
    	int result, cnt;
    	Op op;
    	
    	public MyOperation(Op op) {
    		this.op = op;
    		result = 0;
    		cnt = 0;
    		if(op == Op.MAX) {
    			result = Integer.MIN_VALUE;
    		} else if (op == Op.MIN) {
    			result = Integer.MAX_VALUE;
    		}
    	}
    	public void add(Integer val) {
    		switch(op) {
    		case MAX:
    			result = Math.max(result, val);
    			break;
    		case MIN:
    			result = Math.min(result, val);
    			break;
    		case SUM:
    			result += val;
    			break;
    		case AVG:
    			result += val;
    			cnt += 1;
    			break;
    		case COUNT:
    			cnt += 1;
    			break;
    		default:
    			System.out.println("don't konw which op");
    		}
    	}
    	public Integer get() {
    		switch(op) {
    		case COUNT:
    			return cnt;
    		case AVG:
    			return result/cnt;
    		default:
    			return result;
    		}
    	}
    }
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.op = what;
    	fieldWithTuple = new HashMap<>();
    	fieldWithValue = new HashMap<>();
    	Tuple NoGroupingTuple;
    	if(gbfield == NO_GROUPING) {
    		td = new TupleDesc(new Type[] {Type.INT_TYPE});
    		NoGroupingTuple = new Tuple(td);
    		NoGroupingTuple.setField(0, NO_GROUPING_FIELD);
    		fieldWithTuple.put(NO_GROUPING_FIELD, NoGroupingTuple);
    		fieldWithValue.put(NO_GROUPING_FIELD, new MyOperation(op));
    	} else {
    		td = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});
    	}
    	
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	//now we think about NO_GROUPING
    	int index = (gbfield == NO_GROUPING) ? 0:1;
    	Field field1 = (gbfield == NO_GROUPING)?
    			       NO_GROUPING_FIELD
    			     : tup.getField(gbfield);
    	//IntField aggre_val = (IntField) tup.getField(afield);
    	
    	if(!fieldWithTuple.containsKey(field1)) {
    		fieldWithTuple.put(field1, new Tuple(td));
    		fieldWithValue.put(field1, new MyOperation(op));
    		fieldWithTuple.get(field1).setField(0, field1);
    		fieldWithTuple.get(field1).setField(index, new IntField(0));
    	}
    	IntField field2 = (IntField) tup.getField(afield);
    	fieldWithValue.get(field1).add(field2.getValue());
    	int res = fieldWithValue.get(field1).get();
    	fieldWithTuple.get(field1).setField(index, new IntField(res));
    	
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        Iterable<Tuple> tuples = fieldWithTuple.values();
        return new TupleIterator(td, tuples);
    }

}

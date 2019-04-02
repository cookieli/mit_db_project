package simpledb;

import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;
    private HashMap<Field, Tuple> fieldWithTuple;
    private HashMap<Field, Integer> fieldWithCount;
    private TupleDesc td;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	if(what != Op.COUNT) {
    		throw new IllegalArgumentException("the op isn't COUNT");
    	}
    	this.op = what;
    	fieldWithTuple = new HashMap<>();
    	fieldWithCount = new HashMap<>();
    	Tuple NoGroupingTuple;
    	if(gbfield == NO_GROUPING) {
    		td = new TupleDesc(new Type[] {Type.INT_TYPE});
    		NoGroupingTuple = new Tuple(td);
    		NoGroupingTuple.setField(0, NO_GROUPING_FIELD);
    		fieldWithTuple.put(NO_GROUPING_FIELD, NoGroupingTuple);
    		fieldWithCount.put(NO_GROUPING_FIELD, 0);
    	}else {
    		td = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});
    	}
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	int index = (gbfield == NO_GROUPING) ? 0:1;
    	Field field1 = (gbfield == NO_GROUPING)?
    			       NO_GROUPING_FIELD
    			     : tup.getField(gbfield);
    	if(!fieldWithTuple.containsKey(field1)) {
    		fieldWithTuple.put(field1, new Tuple(td));
    		fieldWithCount.put(field1, 0);
    		fieldWithTuple.get(field1).setField(0, field1);
    		fieldWithTuple.get(field1).setField(index, new IntField(0));
    	}
    	int cnt = fieldWithCount.get(field1) + 1;
    	fieldWithCount.put(field1, cnt);
    	fieldWithTuple.get(field1).setField(index, new IntField(cnt));
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
    	Iterable<Tuple> tuples = fieldWithTuple.values();
        return new TupleIterator(td, tuples);
    }

}

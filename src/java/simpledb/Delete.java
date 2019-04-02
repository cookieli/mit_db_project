package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private TransactionId tid;
    private TupleDesc td;
    private int fetchTime;
    

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
    	this.tid = t;
    	this.child = child;
    	td = new TupleDesc(new Type[] {Type.INT_TYPE});
    	fetchTime = 0;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.open();
    	super.open();
    }

    public void close() {
        // some code goes here
    	super.close();
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.rewind();
    	fetchTime = 0;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if(fetchTime > 0) return null;
    	Tuple res = new Tuple(td);
    	res.setField(0, new IntField(0));
    	if(!child.hasNext())   {
    		fetchTime ++;
    		return res;
    	
    	}
    	
    	while(child.hasNext()) {
    		Tuple t = child.next();
    		try {
				Database.getBufferPool().deleteTuple(tid, t);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		IntField v = (IntField) res.getField(0);
    		res.setField(0, new IntField(v.getValue() + 1));
    	}
    	fetchTime++;
    	return res;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    }

}

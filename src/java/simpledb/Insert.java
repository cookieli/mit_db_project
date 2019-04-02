package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

	private static final long serialVersionUID = 1L;

	private OpIterator child;
	private int tableId;
	private TransactionId tid;
	private TupleDesc td;
	private int fetchtime;

	/**
	 * Constructor.
	 *
	 * @param t       The transaction running the insert.
	 * @param child   The child operator from which to read tuples to be inserted.
	 * @param tableId The table in which to insert tuples.
	 * @throws DbException if TupleDesc of child differs from table into which we
	 *                     are to insert.
	 */
	public Insert(TransactionId t, OpIterator child, int tableId) throws DbException {
		// some code goes here
		this.tid = t;
		this.child = child;
		this.tableId = tableId;
		td = new TupleDesc(new Type[] { Type.INT_TYPE });
		fetchtime = 0;
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
		fetchtime = 0;
	}

	/**
	 * Inserts tuples read from child into the tableId specified by the constructor.
	 * It returns a one field tuple containing the number of inserted records.
	 * Inserts should be passed through BufferPool. An instances of BufferPool is
	 * available via Database.getBufferPool(). Note that insert DOES NOT need check
	 * to see if a particular tuple is a duplicate before inserting it.
	 *
	 * @return A 1-field tuple containing the number of inserted records, or null if
	 *         called more than once.
	 * @throws IOException
	 * @see Database#getBufferPool
	 * @see BufferPool#insertTuple
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		Tuple res = new Tuple(td);
    	res.setField(0, new IntField(0));
    	if(fetchtime == 1) return null;
    	
		if(!child.hasNext())  {
			fetchtime ++;
			return res;
		}
		
		while (child.hasNext()) {
			Tuple t = child.next();
			try {
				Database.getBufferPool().insertTuple(tid, tableId, t);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			IntField i = (IntField) res.getField(0);
			res.setField(0, new IntField(i.getValue() + 1));
			//System.out.println(i.getValue() + 1);
			
		}
		fetchtime ++;
		return res;
	}

	@Override
	public OpIterator[] getChildren() {
		// some code goes here
		return new OpIterator[] { child };
	}

	@Override
	public void setChildren(OpIterator[] children) {
		// some code goes here
		if (this.child != children[0]) {
			this.child = children[0];
		}
	}
}

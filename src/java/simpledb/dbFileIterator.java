package simpledb;

import java.util.NoSuchElementException;

public class dbFileIterator implements  DbFileIterator{
    private	TransactionId id;
	private HeapFile f;
	private long pgNum;
	HeapPage.tupleIterator<Tuple> t;
	//private int tableId;
	
	public dbFileIterator(TransactionId id, HeapFile f) {
		this.id = id;
		this.f = f;
		this.pgNum = 0;
	}
	@Override
	public void open() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		t = getIteratorFromPage(f.getId(), (int)pgNum);
	}

	@Override
	public boolean hasNext() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		if(t == null) {
			return false;
		}
		if(t.hasNext()) {
			return true;
		} else {
			while(pgNum < f.numPages() - 1) {
				pgNum++;
				t = getIteratorFromPage(f.getId(), (int)pgNum);
				if(t == null) {
					return false;
				}
				if(t.hasNext()){
					return true;
				}
			} 
		}
		return false;
	}
	
	public HeapPage.tupleIterator<Tuple> getIteratorFromPage(int tableId, int pgNum) throws TransactionAbortedException, DbException{
		PageId hpID = new HeapPageId(tableId, (int)pgNum);
		HeapPage hp;
		hp = (HeapPage)Database.getBufferPool().getPage(id, hpID, Permissions.READ_ONLY);
		//get tuple iterator from Page
		if(hp.isPageEmpty()) {
			//return null;
		}
		return (simpledb.HeapPage.tupleIterator<Tuple>) hp.iterator();
	}
	@Override
	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
		// TODO Auto-generated method stub
		if(t == null) {
			throw new NoSuchElementException();
		}
		return t.next();
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		close();
		open();
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		pgNum = 0;
		t = null;
		
	}

}

package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	
	final File heapFile;
	final TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.heapFile  = f;
    	this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return heapFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
    	return heapFile.getAbsoluteFile().hashCode();
        //throw new UnsupportedOperationException("implement this");
    }
    

 

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    	return td;
        //throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
    	try {
			RandomAccessFile rcf = new RandomAccessFile(heapFile, "r");
			byte[] temp = new byte[BufferPool.getPageSize()];
			int pgNo = pid.getPageNumber();
	    	long start_pos = pgNo * BufferPool.getPageSize();
	    	rcf.seek(start_pos);
	    	for(int i = 0; i < BufferPool.getPageSize(); i++) {
	    		temp[i] = rcf.readByte();
	    	}
	    	rcf.close();
	    	return new HeapPage((HeapPageId)pid, temp);
	    	
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    	int pgNo = page.getId().getPageNumber();
    	long start_pos = pgNo * BufferPool.getPageSize();
    	RandomAccessFile rcf = new RandomAccessFile(heapFile, "rw");
    	rcf.seek(start_pos);
    	byte[] temp = page.getPageData();
    	rcf.write(temp);
    	rcf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (heapFile.length()/ BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
    	ArrayList<Page> pages = new ArrayList<>();
    	for(int i = 0; i < numPages(); i++) {
    		//System.out.println("it has "+i+"pages");
    		PageId pid = new HeapPageId(this.getId(), i);
    		HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
    		if(p.getNumEmptySlots() > 0) {
    			p.insertTuple(t);
    			pages.add(p);
    			break;
    		}
    	}
    	if(pages.isEmpty()) {
    		//throw new DbException("can't find page to insert");
    		HeapPageId pid = new HeapPageId(this.getId(), numPages());
    		HeapPage p = new HeapPage(pid, HeapPage.createEmptyPageData());
    		p.insertTuple(t);
    		this.writePage(p);
    		pages.add(p);
    	}
        return pages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
    	ArrayList<Page> pages = new ArrayList<>();
    	PageId t_pid = t.getRecordId().getPageId();
    	for(int i = 0; i < numPages(); i++) {
    		PageId pid = new HeapPageId(this.getId(), i);
    		if(pid.equals(t_pid)) {
    			HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
    			p.deleteTuple(t);
    			pages.add(p);
    		}
    	}
        return pages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new dbFileIterator(tid, this);
    }
    
    
    	
    
    
    

}


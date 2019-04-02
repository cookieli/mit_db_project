package simpledb;

import java.io.Serializable;
import java.util.Iterator;
import simpledb.Field;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    public TupleDesc schema;
    public Field[] values;
    public RecordId rid;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
    	schema = td;
    	values = new Field[td.numFields()];
    	rid = null;
    }
    
    public boolean equals(Object o) {
    	if(o == null)
    		return false;
    	if (o == this)
    		return true;
    	if (!(o instanceof Tuple))
    		return false;
    	Tuple t = (Tuple) o;
    	if(rid.equals(t.getRecordId()) && t.getTupleDesc().equals(schema)) {
    		return true;
    	}
    	return false;
    }
    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return schema;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
    	this.rid = rid;
    }
    
    public int numOfFields() {
    	return schema.numFields();
    }
    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    
    
    public void setField(int i, Field f) {
    	// some code goes here
    	if(i < 0 || i > numOfFields()) {
    		throw new IllegalArgumentException("i is out of fields");
    	}
    	values[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
    	if(i < 0 || i > numOfFields()) {
    		throw new IllegalArgumentException("i is out of fields");
    	}
        return values[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
    	String ret = "";
    	ret += schema.toString();
    	ret += "\n";
    	for(int i =0; i < numOfFields(); i++) {
    		ret += values[i].toString()+"\t";
    	}
    	return ret;
        //throw new UnsupportedOperationException("Implement this");
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return new FieldIterator<>();
    }
    
    class FieldIterator<Field> implements Iterator<Field>{
    	int i = 0;
		@Override
		public boolean hasNext() {
			// TODO Auto-generated method stub
			return i < values.length;
		}

		@Override
		public Field next() {
			// TODO Auto-generated method stub
			return   (Field) values[i++];
		}
    	
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
    	schema = td;
    	values = new Field[td.numFields()];
    }
}

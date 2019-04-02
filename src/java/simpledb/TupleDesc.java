package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
	
	public TDItem items[];

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
        
        public int size() {
        	return fieldType.getLen();
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return new TDItemIterator<>();
    }
	class TDItemIterator<TDItem> implements Iterator<TupleDesc.TDItem>{
    	int i = 0;
		@Override
		public boolean hasNext() {
			// TODO Auto-generated method stub
			return i < items.length;
		}

		@Override
		public TupleDesc.TDItem next() {
			// TODO Auto-generated method stub
			return items[i++];
		
		}
    	
    }
    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
    	if(typeAr.length == 0) {
    		throw new IllegalArgumentException("the typeAr length must contain at least one entry");
    	}
    	if(typeAr.length != fieldAr.length) {
    		throw new IllegalArgumentException("two array's length doesn't match");
    	}
    	items = new TDItem[typeAr.length];
    	for(int i = 0; i < typeAr.length; i++) {
    		items[i] = new TDItem(typeAr[i], fieldAr[i]);
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
    	this(typeAr, new String[typeAr.length]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return items.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        return items[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        return items[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
    	int i;
    	if(name == null) {
    		throw new NoSuchElementException("no element of null");
    	}
    	if(allFieldNameAreNonvalid()) {
    		throw new NoSuchElementException("all field name are null");
    	}
    	
    	
    	for(i = 0; i < items.length; i++) {
    		if(items[i].fieldName.equals(name)) {
    			return i;
    		}
    	}
    	if(i == items.length) {
    		throw new NoSuchElementException("no element of" + name);
    	}
    	return -1;
    }
    
    public boolean allFieldNameAreNonvalid() {
    	for(int i = 0; i < numFields(); i++) {
    		if (items[i].fieldName != null) {
    			return false;
    		}
    	}
    	return true;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
    	int sum = 0;
    	for(int i = 0; i< items.length; i++) {
    		sum += items[i].size();
    	}
        return sum;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
    	Type TypeAr[] = new Type[td1.numFields() + td2.numFields()];
    	
    	String fieldAr[] = new String[TypeAr.length];
    	for(int i = 0; i < td1.numFields(); i++) {
    		TypeAr[i] = td1.getFieldType(i);
    		fieldAr[i] = td1.getFieldName(i);
    	}
    	for(int i = td1.numFields(); i < TypeAr.length; i++) {
    		int j = i - td1.numFields();
    		TypeAr[i] = td2.getFieldType(j);
    		fieldAr[i] = td2.getFieldName(j);
    	}
    	TupleDesc newTupleDesc = new TupleDesc(TypeAr, fieldAr);
        return newTupleDesc;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

	public boolean equals(Object o) {
		// some code goes here
		if(o == null) {
			return false;
		}
		if(o == this) {
			return true;
		}
		if( !(o instanceof TupleDesc)) {
			return false;
		}
		TupleDesc another = (TupleDesc) o;
		if (numFields() == another.numFields()) {
			for (int i = 0; i < numFields(); i++) {
				if (items[i].fieldType != another.getFieldType(i)) {
					return false;
				}
			}
			return true;
		} else {
			return false;
		}
	}

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        //throw new UnsupportedOperationException("unimplemented");
    	int ret = 0;
    	for(int i = 0; i < numFields(); i++) {
    		if(items[i].fieldType == Type.INT_TYPE) {
    			ret += 2;
    		} else {
    			ret += 3;
    		}
    	}
    	return ret;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
    	String ret = "";
    	for(int i = 0; i < numFields(); i++) {
    		ret += items[i].toString()+ " ";
    	}
        return  ret;
    }
    public static void main(String args[]) {
    	Type[] typeAr = new Type[] {Type.INT_TYPE, Type.INT_TYPE};
    	String[] fieldAr = new String[] {"a", "b"};
    	TupleDesc s = new TupleDesc(typeAr, fieldAr);
    	Iterator<TupleDesc.TDItem> it = s.iterator();
    	while(it.hasNext()) {
    		System.out.println(it.next().fieldName);
    	}
    }
}

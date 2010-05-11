package nl.thanod.cassandra;

public interface Storeable {
	/** 
	 * @return the key to store the object value's under in cassandra
	 */
	byte [] getKey();
}

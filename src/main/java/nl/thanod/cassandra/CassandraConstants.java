package nl.thanod.cassandra;

public class CassandraConstants {
	public static final byte [] EMPTY_BYTES = new byte[0];
	public static final String EMPTY_STRING = "";
	
	public static long getTime() {
		return System.currentTimeMillis() * 1000;
	}
}

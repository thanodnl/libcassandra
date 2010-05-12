package nl.thanod.cassandra;

import java.util.Iterator;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;

public class ColumnFamilyObjectStore<T> implements ObjectStore<T> {

	private final Client client;
	private final String keyspace;
	private final String column_family;
	private final Class<T> type;

	public ColumnFamilyObjectStore(Cassandra.Client client, String keyspace, String column_family, Class<T> type){
		this.client = client;
		this.keyspace = keyspace;
		this.column_family = column_family;
		this.type = type;
	}
	
	@Override
	public T load(byte[] key) {
		return null;
	}

	@Override
	public void store(T object) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Iterator<T> iterator() {
		// TODO Auto-generated method stub
		return null;
	}

}

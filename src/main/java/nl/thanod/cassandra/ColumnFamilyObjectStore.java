package nl.thanod.cassandra;

import java.util.Iterator;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.TException;

public class ColumnFamilyObjectStore<T> implements ObjectStore<T> {

	private final Client client;
	private final String keyspace;
	private final String column_family;
	private final Class<T> type;

	public ColumnFamilyObjectStore(Cassandra.Client client, String keyspace, String column_family, Class<T> type) {
		this.client = client;
		this.keyspace = keyspace;
		this.column_family = column_family;
		this.type = type;
	}

	@Override
	public T load(byte[] key) {
		return Store.load(client, keyspace, column_family, key, type);
	}

	@Override
	public void store(T object) {
//		try {
//			Store.store(client, keyspace, column_family, ConsistencyLevel.ONE, object);
//		} catch (InvalidRequestException ball) {
//			ball.printStackTrace();
//		} catch (UnavailableException ball) {
//			ball.printStackTrace();
//		} catch (TimedOutException ball) {
//			ball.printStackTrace();
//		} catch (TException ball) {
//			ball.printStackTrace();
//		}
	}

	@Override
	public Iterator<T> iterator() {
		// TODO Auto-generated method stub
		return null;
	}

}

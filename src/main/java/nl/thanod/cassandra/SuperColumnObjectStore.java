package nl.thanod.cassandra;

import java.util.*;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.TException;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class SuperColumnObjectStore<T> implements ObjectStore<T> {
	public static int RANGE_SIZE = 100;
	private final Client client;
	private final Class<T> type;
	private final String keyspace;
	private final String column_family;
	private final String super_column;

	public SuperColumnObjectStore(Cassandra.Client client, String keyspace, String column_family, String super_column, Class<T> type) {
		this.client = client;
		this.type = type;
		this.keyspace = keyspace;
		this.column_family = column_family;
		this.super_column = super_column;
	}

	@Override
	public T load(byte[] key) {
		return Store.load(client, keyspace, column_family, super_column, key, type);
	}
	
	public void add(Object o) throws InvalidRequestException, UnavailableException, TimedOutException, TException{
		Store.store(client, keyspace, column_family, ConsistencyLevel.ONE, o);
	}

	@Override
	public void store(T object) {
		try {
			Store.store(client, keyspace, column_family, super_column, ConsistencyLevel.ONE, object);
		} catch (InvalidRequestException ball) {
			ball.printStackTrace();
		} catch (UnavailableException ball) {
			ball.printStackTrace();
		} catch (TimedOutException ball) {
			ball.printStackTrace();
		} catch (TException ball) {
			ball.printStackTrace();
		}
	}

	@Override
	public Iterator<T> iterator() {
		try {
			return new SuperColumnObjectIterator<T>(this.client, this.keyspace, this.column_family, this.super_column, this.type);
		} catch (Throwable ball) {
			throw new RuntimeException("Could not fetch an iterator for " + keyspace + "." + column_family + "['" + super_column + "']", ball);
		}
	}

	static class SuperColumnObjectIterator<T> implements Iterator<T> {
		public static final byte[] EMPTY = new byte[0];

		private final Client client;
		private final Class<T> type;
		private final String keyspace;
		private final String column_family;
		private final String super_column;
		private final Queue<T> buffer;
		private final boolean reversed;
		private byte[] lastKey;

		SuperColumnObjectIterator(Cassandra.Client client, String keyspace, String column_family, String super_column, Class<T> type) throws InvalidRequestException, UnavailableException, TimedOutException, TException {
			this(client, keyspace, column_family, super_column, true, type);
		}

		SuperColumnObjectIterator(Cassandra.Client client, String keyspace, String column_family, String super_column, boolean reversed, Class<T> type) throws InvalidRequestException, UnavailableException, TimedOutException, TException {
			this.client = client;
			this.type = type;
			this.keyspace = keyspace;
			this.column_family = column_family;
			this.super_column = super_column;
			this.buffer = new LinkedList<T>();
			this.reversed = reversed;
			this.lastKey = EMPTY;
		}

		private void fillBuffer() {
			try {
				ColumnParent column_parent = new ColumnParent(column_family);
				SlicePredicate predicate = new SlicePredicate();
				predicate.slice_range = new SliceRange(this.lastKey, EMPTY, this.reversed, SuperColumnObjectStore.RANGE_SIZE);
				List<ColumnOrSuperColumn> list = client.get_slice(keyspace, super_column, column_parent, predicate, ConsistencyLevel.ONE);
				if (!Arrays.equals(this.lastKey, EMPTY))
					list.remove(0);
				for (ColumnOrSuperColumn corsc : list) {
					buffer.add(Store.load(corsc.super_column, this.type));
					this.lastKey = corsc.super_column.name;
				}
			} catch (Throwable ball) {
				ball.printStackTrace();
			}
		}

		@Override
		public boolean hasNext() {
			if (this.buffer.size() <= 0)
				fillBuffer();
			return this.buffer.size() > 0;
		}

		@Override
		public T next() {
			if (this.buffer.size() <= 0)
				fillBuffer();
			return this.buffer.poll();
		}

		@Override
		public void remove() {
			throw new NotImplementedException();
		}

	}
}

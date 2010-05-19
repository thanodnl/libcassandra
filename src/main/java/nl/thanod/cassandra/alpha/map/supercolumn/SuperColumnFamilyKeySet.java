package nl.thanod.cassandra.alpha.map.supercolumn;

import java.util.*;

import nl.thanod.cassandra.CassandraConstants;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.thrift.Cassandra.Iface;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class SuperColumnFamilyKeySet implements Set<String> {
	public static int BUFFER_SIZE = 100;
	private final Iface client;
	private final String keyspace;
	private final String column_family;

	private final ConsistencyLevel readLevel;
	private final ConsistencyLevel writeLevel;

	public SuperColumnFamilyKeySet(Cassandra.Iface client, String keyspace, String column_family) {
		this(client, keyspace, column_family, ConsistencyLevel.ANY);
	}

	public SuperColumnFamilyKeySet(Cassandra.Iface client, String keyspace, String column_family, ConsistencyLevel consistency_level) {
		this(client, keyspace, column_family, consistency_level, consistency_level);
	}

	public SuperColumnFamilyKeySet(Cassandra.Iface client, String keyspace, String column_family, ConsistencyLevel readLevel, ConsistencyLevel writeLevel) {
		this.client = client;
		this.keyspace = keyspace;
		this.column_family = column_family;

		this.readLevel = readLevel;
		this.writeLevel = writeLevel;
	}

	@Override
	public boolean add(String e) {
		throw new UnsupportedOperationException("Unable to add things to a SuperColumnFamilyKeySet");
	}

	@Override
	public boolean addAll(Collection<? extends String> c) {
		throw new UnsupportedOperationException("Unable to add things to a SuperColumnFamilyKeySet");
	}

	@Override
	public void clear() {
		Iterator<String> keys = this.iterator();
		while (keys.hasNext()) {
			keys.next();
			keys.remove();
		}
	}

	@Override
	public boolean contains(Object o) {
		if (!(o instanceof String))
			return false;
		// because every key is contained, but maps returned could be empty
		return true;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		for (Object o : c)
			if (!this.contains(o))
				return false;
		return true;
	}

	@Override
	public boolean isEmpty() {
		// TODO Auto-generated method stub
		throw new NotImplementedException();
	}

	@Override
	public Iterator<String> iterator() {
		return new SuperColumnFamilyKeyIterator(client, keyspace, column_family, readLevel, writeLevel);
	}

	@Override
	public boolean remove(Object o) {
		if (!(o instanceof String))
			return false;
		try {
			client.remove(this.keyspace, (String) o, new ColumnPath(this.column_family), CassandraConstants.getTime(), this.writeLevel);
			return true;
		} catch (Throwable ball) {
			throw new RuntimeException("Unable to recover from " + ball.getClass().getCanonicalName(), ball);
		}
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException("Unable to remove things from a SuperColumnFamilyKeySet");
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException("Unable to remove things from a SuperColumnFamilyKeySet");
	}

	@Override
	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Object[] toArray() {
		throw new NotImplementedException();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		throw new NotImplementedException();
	}

	static class SuperColumnFamilyKeyIterator implements Iterator<String> {
		private final Iface client;
		private final String keyspace;
		private final String column_family;
		private final Queue<String> buffer;

		private final ConsistencyLevel readLevel;
		private final ConsistencyLevel writeLevel;

		private String lastBufferedKey = CassandraConstants.EMPTY_STRING;
		private boolean finished = false;
		private String readLast = null;

		public SuperColumnFamilyKeyIterator(Cassandra.Iface client, String keyspace, String column_family, ConsistencyLevel readLevel, ConsistencyLevel writeLevel) {
			this.client = client;
			this.keyspace = keyspace;
			this.column_family = column_family;
			this.buffer = new LinkedList<String>();

			this.readLevel = readLevel;
			this.writeLevel = writeLevel;
		}

		private void fillBuffer() {
			if (finished)
				return;
			try {
				ColumnParent column_parent = new ColumnParent(this.column_family);
				SlicePredicate predicate = new SlicePredicate();
				predicate.slice_range = new SliceRange(CassandraConstants.EMPTY_BYTES, CassandraConstants.EMPTY_BYTES, false, 1);
				
				// add one to the buffersize because you can find lastBufferedKey at the first place of the returned list
				List<KeySlice> list = client.get_range_slice(this.keyspace, column_parent, predicate, lastBufferedKey, CassandraConstants.EMPTY_STRING, SuperColumnFamilyKeySet.BUFFER_SIZE+1, this.readLevel);
				
				// if the first key is the one starting on, remove it
				if (list.size() > 0 && lastBufferedKey.equals(list.get(0).key)) {
					list.remove(0);
				}
				if (list.size() == 0) {
					finished = true;
				} else {
					for (KeySlice k : list) {
						if (k.columns.size() != 0)
							buffer.add(k.key);
						lastBufferedKey = k.key;
					}
				}
			} catch (Throwable ball) {
				//TODO find a way to handle the connection drops
				ball.printStackTrace();
			}
		}

		@Override
		public boolean hasNext() {
			while (!finished && buffer.size() == 0)
				fillBuffer();
			return buffer.size() > 0;
		}

		@Override
		public String next() {
			if (hasNext())
				return readLast = buffer.poll();
			else
				readLast = null;
			throw new NoSuchElementException();
		}

		@Override
		public void remove() {
			System.out.println("removing " + readLast);
			if (readLast == null)
				throw new IllegalStateException();
			try {
				client.remove(keyspace, readLast, new ColumnPath(column_family), CassandraConstants.getTime(), writeLevel);
			} catch (Throwable ball) {
				throw new RuntimeException("Unrecoverable exception " + ball.getClass().getCanonicalName(), ball);
			}
		}
	}
}

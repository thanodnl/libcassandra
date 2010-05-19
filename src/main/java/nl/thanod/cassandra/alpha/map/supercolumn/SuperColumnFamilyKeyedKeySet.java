package nl.thanod.cassandra.alpha.map.supercolumn;

import java.util.*;

import javax.management.RuntimeErrorException;

import nl.thanod.cassandra.CassandraConstants;
import nl.thanod.cassandra.alpha.bytes.ByteObjectTranslator;
import nl.thanod.cassandra.alpha.bytes.NoTranslatorException;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.thrift.Cassandra.Iface;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public final class SuperColumnFamilyKeyedKeySet<K> implements Set<K> {
	public static int BUFFER_SIZE = 100;

	protected final ByteObjectTranslator<K> translator;
	protected final Iface client;
	protected final String keyspace;
	protected final String column_family;
	protected final String key;
	protected final ConsistencyLevel readLevel;
	protected final ConsistencyLevel writeLevel;

	protected final boolean reversed = false;

	public SuperColumnFamilyKeyedKeySet(Class<K> keyVal, Cassandra.Iface client, String keyspace, String column_family, String key) throws NoTranslatorException {
		this(keyVal, client, keyspace, column_family, key, ConsistencyLevel.ONE, ConsistencyLevel.ZERO);
	}

	public SuperColumnFamilyKeyedKeySet(Class<K> keyVal, Cassandra.Iface client, String keyspace, String column_family, String key, ConsistencyLevel level) throws NoTranslatorException {
		this(keyVal, client, keyspace, column_family, key, level, level);
	}

	public SuperColumnFamilyKeyedKeySet(Class<K> keyVal, Cassandra.Iface client, String keyspace, String column_family, String key, ConsistencyLevel readLevel, ConsistencyLevel writeLevel) throws NoTranslatorException {
		this(ByteObjectTranslator.getTranslatorFor(keyVal), client, keyspace, column_family, key, readLevel, writeLevel);
	}

	public SuperColumnFamilyKeyedKeySet(ByteObjectTranslator<K> translator, Cassandra.Iface client, String keyspace, String column_family, String key, ConsistencyLevel readLevel, ConsistencyLevel writeLevel) {
		if (translator == null)
			throw new NullPointerException();
		this.translator = translator;
		this.client = client;
		this.keyspace = keyspace;
		this.column_family = column_family;
		this.key = key;

		this.readLevel = readLevel;
		this.writeLevel = writeLevel;
	}

	@Override
	public boolean add(K e) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean addAll(Collection<? extends K> c) {
		for (K o : c)
			if (!add(o))
				return false;
		return true;
	}

	@Override
	public void clear() {
		Iterator<K> it = this.iterator();
		while (it.hasNext()) {
			it.next();
			it.remove();
		}
	}

	@Override
	public boolean contains(Object o) {
		throw new NotImplementedException();
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		for (Object o : c)
			if (!contains(o))
				return false;
		return true;
	}

	@Override
	public boolean isEmpty() {
		throw new NotImplementedException();
	}

	@Override
	public Iterator<K> iterator() {
		throw new NotImplementedException();
	}

	@Override
	public boolean remove(Object o) {
		throw new NotImplementedException();
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		for (Object o : c)
			if (!remove(o))
				return false;
		return true;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int size() {
		throw new NotImplementedException();
	}

	@Override
	public Object[] toArray() {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		throw new UnsupportedOperationException();
	}

	class KeyedKeyIterator implements Iterator<K> {

		private final Queue<K> buffer = new LinkedList<K>();
		private K working = null;
		private boolean finished = false;
		private byte[] lastKey = CassandraConstants.EMPTY_BYTES;

		private void fillBuffer() {
			if (finished)
				return;
			try {
				ColumnParent column_parent = new ColumnParent(column_family);
				column_parent.column_family = key;
				SlicePredicate predicate = new SlicePredicate();
				predicate.slice_range = new SliceRange(lastKey, CassandraConstants.EMPTY_BYTES, reversed, BUFFER_SIZE);

				List<ColumnOrSuperColumn> corsc = client.get_slice(keyspace, key, column_parent, predicate, readLevel);
				if (corsc.size() > 0) {
					ColumnOrSuperColumn sc = corsc.get(0);
					if (sc.super_column == null)
						throw new RuntimeException("A SuperColumn was expexted");
					else if (Arrays.equals(sc.super_column.name, lastKey))
						corsc.remove(0);
				}
				if (corsc.size() > 0) {
					for (ColumnOrSuperColumn c : corsc) {
						if (c.super_column == null)
							throw new RuntimeException("A SuperColumn was expexted");
						lastKey = c.super_column.name;
						if (c.super_column.columns.size() > 0)
							buffer.add(translator.get(c.super_column.name));
					}
				} else {
					finished = true;
				}
			} catch (Throwable ball) {
				throw new RuntimeException("Unable to recover from " + ball.getClass().getCanonicalName(), ball);
			}
		}

		@Override
		public boolean hasNext() {
			if (buffer.size() == 0)
				fillBuffer();
			return buffer.size() > 0;
		}

		@Override
		public K next() {
			if (hasNext())
				return working = this.buffer.poll();
			throw new NoSuchElementException();
		}

		@Override
		public void remove() {
			throw new NotImplementedException();
		}

	}
}

package nl.thanod.cassandra.alpha.map;

import java.util.*;

import javax.imageio.spi.ServiceRegistry;

import nl.thanod.cassandra.CassandraConstants;
import nl.thanod.cassandra.bytes.ByteTranslator;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.thrift.Cassandra.Client;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public final class ColumnFamilyMap<K, V> implements Map<K, V> {
	protected static final byte[] EMPTY = new byte[0];
	public static int FETCH_SIZE = 100;

	protected final Client client;
	protected final String keyspace;
	protected final String key;
	protected final String column_family;
	protected final ConsistencyLevel consistency_level;
	protected final ByteTranslator valueTranslator;
	protected final ByteTranslator keyTranslator;

	protected final boolean reversed;

	public ColumnFamilyMap(final Class<K> keyType, final Class<V> valType, final Cassandra.Client client, final String keyspace, final String column_family, final String key, final ConsistencyLevel consistency_level) {
		this(keyType, valType, client, keyspace, column_family, key, consistency_level, false);
	}

	public ColumnFamilyMap(final Class<K> keyType, final Class<V> valType, final Cassandra.Client client, final String keyspace, final String column_family, final String key, final ConsistencyLevel consistency_level, boolean reversed) {
		Iterator<ByteTranslator> list = ServiceRegistry.lookupProviders(ByteTranslator.class);
		ByteTranslator trans = null;
		ByteTranslator valtrans = null;
		ByteTranslator keytrans = null;
		while (list.hasNext()) {
			trans = list.next();
			if (trans.canTranslate(valType))
				valtrans = trans;
			if (trans.canTranslate(keyType))
				keytrans = trans;
		}
		if (valtrans == null)
			throw new RuntimeException("No translator found for " + valType.getCanonicalName());
		if (keytrans == null)
			throw new RuntimeException("No translator found for " + keyType.getCanonicalName());

		this.client = client;
		this.key = key;
		this.keyspace = keyspace;
		this.column_family = column_family;
		this.consistency_level = consistency_level;

		this.keyTranslator = keytrans;
		this.valueTranslator = valtrans;
		this.reversed = reversed;
	}

	@Override
	public void clear() {
		try {
			client.remove(keyspace, key, new ColumnPath(column_family), CassandraConstants.getTime(), consistency_level);
		} catch (Throwable ball) {
			// InvalidRequestException, UnavailableException, TimedOutException, TException
			throw new RuntimeException("Unrecoverable exception " + ball.getClass().getCanonicalName(), ball);
		}
	}

	@Override
	public boolean containsKey(Object o) {
		try {
			ColumnPath path = new ColumnPath(column_family);
			path.column = ObjectToByteArray(o);
			ColumnOrSuperColumn cosc = client.get(keyspace, key, path, consistency_level);
			return cosc.column != null;
		} catch (NotFoundException ball) {
			return false;
		} catch (Throwable ball) {
			throw new RuntimeException("Error while looking for the appearance of " + o, ball);
		}
	}

	@Override
	public boolean containsValue(Object paramObject) {
		throw new UnsupportedOperationException("Unable to performe with a Cassandra Backend");
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return new EntrySet<K, V>(this);
	}

	@Override
	public V get(Object o) {
		try {
			ColumnPath path = new ColumnPath(column_family);
			path.column = ObjectToByteArray(o);
			ColumnOrSuperColumn cosc = client.get(keyspace, key, path, consistency_level);
			return (V) valueTranslator.getObject(cosc.column.value);
		} catch (NotFoundException ball) {
			return null;
		} catch (Throwable ball) {
			throw new RuntimeException("Error while fetching object stored under " + o, ball);
		}
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public Set<K> keySet() {
		throw new NotImplementedException();
	}

	@Override
	public V put(K name, V value) {
		try {
			ColumnPath column_path = new ColumnPath(column_family);
			column_path.column = keyTranslator.getBytes(name);
			client.insert(keyspace, key, column_path, valueTranslator.getBytes(value), CassandraConstants.getTime(), consistency_level);
			return value;
		} catch (Throwable ball) {
			throw new RuntimeException("Unable to recover from " + ball.getClass().getCanonicalName(), ball);
		}
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> map) {
		for (Entry<? extends K, ? extends V> e : map.entrySet())
			put(e.getKey(), e.getValue());
	}

	@Override
	public V remove(Object o) {
		V val = get(o);
		if (val == null)
			return null;
		try {
			ColumnPath column_path = new ColumnPath(column_family);
			column_path.column = keyTranslator.getBytes(o);
			client.remove(keyspace, key, column_path, CassandraConstants.getTime(), consistency_level);
			return val;
		} catch (Throwable ball) {
			throw new RuntimeException("Unable to recover from " + ball.getClass().getCanonicalName(), ball);
		}
	}

	@Override
	public int size() {
		try {
			return client.get_count(keyspace, key, new ColumnParent(column_family), consistency_level);
		} catch (Throwable ball) {
			throw new RuntimeException("Unable to recover from " + ball.getClass().getCanonicalName(), ball);
		}
	}

	@Override
	public Collection<V> values() {
		throw new NotImplementedException();
	}

	public static byte[] ObjectToByteArray(Object o) {
		Class<?> type = o.getClass();
		Iterator<ByteTranslator> list = ServiceRegistry.lookupProviders(ByteTranslator.class);
		while (list.hasNext()) {
			ByteTranslator trans = list.next();
			if (trans.canTranslate(type))
				return trans.getBytes(o);
		}
		return null;
	}

	static class EntrySet<K, V> implements Set<Entry<K, V>> {

		private final ColumnFamilyMap<K, V> parent;

		public EntrySet(ColumnFamilyMap<K, V> parent) {
			this.parent = parent;
		}

		@Override
		public boolean add(java.util.Map.Entry<K, V> paramE) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean addAll(Collection<? extends java.util.Map.Entry<K, V>> paramCollection) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void clear() {
			parent.clear();
		}

		@Override
		public boolean contains(Object o) {
			if (!(o instanceof Entry))
				return false;
			Entry<?, ?> e = (Entry) o;
			return parent.containsKey(e.getKey());
		}

		@Override
		public boolean containsAll(Collection<?> paramCollection) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean isEmpty() {
			return parent.isEmpty();
		}

		@Override
		public Iterator<java.util.Map.Entry<K, V>> iterator() {
			return new EntryIterator<K, V>(this.parent);
		}

		@Override
		public boolean remove(Object paramObject) {
			throw new NotImplementedException();
		}

		@Override
		public boolean removeAll(Collection<?> oo) {
			for (Object o : oo)
				if (!remove(o))
					return false;
			return true;
		}

		@Override
		public boolean retainAll(Collection<?> paramCollection) {
			throw new NotImplementedException();
		}

		@Override
		public int size() {
			return parent.size();
		}

		@Override
		public Object[] toArray() {
			throw new NotImplementedException();
		}

		@Override
		public <T> T[] toArray(T[] paramArrayOfT) {
			throw new NotImplementedException();
		}

	}

	static class EntryIterator<K, V> implements Iterator<Entry<K, V>> {
		private final ColumnFamilyMap<K, V> parent;

		Queue<Entry<K, V>> buffer = new LinkedList<Entry<K, V>>();
		private Entry<K, V> working = null;
		private byte[] last = EMPTY;
		private boolean finished = false;

		public EntryIterator(ColumnFamilyMap<K, V> parent) {
			this.parent = parent;
		}

		@Override
		public boolean hasNext() {
			if (buffer.size() == 0)
				fillBuffer();
			return buffer.size() > 0;
		}

		private void fillBuffer() {
			if (finished)
				return;
			try {
				ColumnParent column_parent = new ColumnParent(parent.column_family);
				SlicePredicate predicate = new SlicePredicate();
				predicate.slice_range = new SliceRange(last, EMPTY, parent.reversed, ColumnFamilyMap.FETCH_SIZE);
				List<ColumnOrSuperColumn> lcosc = parent.client.get_slice(parent.keyspace, parent.key, column_parent, predicate, parent.consistency_level);
				if (!Arrays.equals(EMPTY, last)) {
					if (lcosc.size() > 0)
						lcosc.remove(0);
				}
				for (ColumnOrSuperColumn cosc : lcosc) {
					if (cosc.column == null)
						throw new RuntimeException("Expected Columns instead of SuperColumns");
					buffer.add(new ColumnEntry<K, V>(parent, (K) parent.keyTranslator.getObject(cosc.column.name), (V) parent.valueTranslator.getObject(cosc.column.value)));
					last = cosc.column.name;
				}
				if (lcosc.size() == 0)
					finished = true;
			} catch (Throwable ball) {
				throw new RuntimeException("Unable to recover from " + ball.getClass().getCanonicalName(), ball);
			}
		}

		@Override
		public java.util.Map.Entry<K, V> next() {
			if (hasNext()) {
				this.working = buffer.poll();
				return this.working;
			}
			return null;
		}

		@Override
		public void remove() {
			if (this.working != null)
				parent.remove(this.working.getKey());
		}

	}

	static class ColumnEntry<K, V> implements Entry<K, V> {
		private final Map<K, V> parent;
		private final K key;
		private V value;

		public ColumnEntry(Map<K, V> parent, K key, V value) {
			this.parent = parent;
			this.key = key;
			this.value = value;
		}

		@Override
		public K getKey() {
			return key;
		}

		@Override
		public V getValue() {
			return value;
		}

		@Override
		public V setValue(V value) {
			return this.value = parent.put(getKey(), value);
		}

	}
}

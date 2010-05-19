package nl.thanod.cassandra.alpha.map.supercolumn;

import java.util.*;

import nl.thanod.cassandra.CassandraConstants;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.thrift.Cassandra.Iface;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public final class SuperColumnFamilyMap<K1, K2, V> implements Map<String, Map<K1, Map<K2, V>>> {

	protected final Class<K1> key1Type;
	protected final Class<K2> key2Type;
	protected final Class<V> valType;

	protected final Iface client;
	protected final String keyspace;
	protected final String column_family;

	protected final ConsistencyLevel readLevel;
	protected final ConsistencyLevel writeLevel;

	public SuperColumnFamilyMap(Class<K1> key1Type, Class<K2> key2Type, Class<V> valType, Cassandra.Iface client, String keyspace, String column_family, ConsistencyLevel readLevel, ConsistencyLevel writeLevel) {
		// the type classes to use in ancestors
		this.key1Type = key1Type;
		this.key2Type = key2Type;
		this.valType = valType;

		this.client = client;
		this.keyspace = keyspace;
		this.column_family = column_family;

		this.readLevel = readLevel;
		this.writeLevel = writeLevel;
	}

	@Override
	public void clear() {
		Iterator<String> keys = this.keySet().iterator();
		while (keys.hasNext()) {
			keys.next();
			keys.remove();
		}
	}

	@Override
	public boolean containsKey(Object o) {
		if (!(o instanceof String))
			return false;
		String key = (String) o;
		try {
			ColumnParent column_parent = new ColumnParent(this.column_family);
			SlicePredicate predicate = new SlicePredicate();
			predicate.slice_range = new SliceRange(CassandraConstants.EMPTY_BYTES, CassandraConstants.EMPTY_BYTES, false, 1);
			List<KeySlice> list = client.get_range_slice(this.keyspace, column_parent, predicate, key, key, 1, this.readLevel);
			return list.size() > 0 && list.get(0).columns.size() > 0;
		} catch (Throwable ball) {
			throw new RuntimeException("Unable to recover from " + ball.getClass().getCanonicalName(), ball);
		}
	}

	@Override
	public boolean containsValue(Object value) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Not possible with cassandra as a backend");
	}

	@Override
	public Set<java.util.Map.Entry<String, Map<K1, Map<K2, V>>>> entrySet() {
		// TODO Auto-generated method stub
		throw new NotImplementedException();
	}

	@Override
	public Map<K1, Map<K2, V>> get(Object key) {
		// TODO Auto-generated method stub
		throw new NotImplementedException();
	}

	@Override
	public boolean isEmpty() {
		// TODO Auto-generated method stub
		throw new NotImplementedException();
	}

	@Override
	public Set<String> keySet() {
		return new SuperColumnFamilyKeySet(client, keyspace, column_family, readLevel, writeLevel);
	}

	@Override
	public Map<K1, Map<K2, V>> put(String key, Map<K1, Map<K2, V>> value) {
		// TODO Auto-generated method stub
		throw new NotImplementedException();
	}

	@Override
	public void putAll(Map<? extends String, ? extends Map<K1, Map<K2, V>>> m) {
		// TODO Auto-generated method stub
		throw new NotImplementedException();
	}

	@Override
	public Map<K1, Map<K2, V>> remove(Object key) {
		// TODO Auto-generated method stub
		throw new NotImplementedException();
	}

	@Override
	public int size() {
		// TODO Auto-generated method stub
		throw new NotImplementedException();
	}

	@Override
	public Collection<Map<K1, Map<K2, V>>> values() {
		// TODO Auto-generated method stub
		throw new NotImplementedException();
	}

	class EntrySet implements Set<Map.Entry<String, Map<K1, Map<K2, V>>>> {

		private final Set<String> keys;

		public EntrySet() {
			keys = SuperColumnFamilyMap.this.keySet();
		}

		@Override
		public boolean add(java.util.Map.Entry<String, Map<K1, Map<K2, V>>> arg0) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean addAll(Collection<? extends java.util.Map.Entry<String, Map<K1, Map<K2, V>>>> c) {
			for (Map.Entry<String, Map<K1, Map<K2, V>>> e : c)
				if (!add(e))
					return false;
			return true;
		}

		@Override
		public void clear() {
			SuperColumnFamilyMap.this.clear();
		}

		@Override
		public boolean contains(Object thing) {
			Object o = thing;
			if (o instanceof Map.Entry<?, ?>)
				o = ((Map.Entry<?, ?>) o).getKey();
			return SuperColumnFamilyMap.this.containsKey(o);
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
			return keys.isEmpty();
		}

		@Override
		public Iterator<java.util.Map.Entry<String, Map<K1, Map<K2, V>>>> iterator() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean remove(Object arg0) {
			Object o = arg0;
			if (o instanceof Map.Entry<?, ?>)
				o = ((Map.Entry<?, ?>) o).getKey();
			return SuperColumnFamilyMap.this.remove(o) != null;
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
			throw new NotImplementedException();
		}

		@Override
		public int size() {
			return keys.size();
		}

		@Override
		public Object[] toArray() {
			throw new NotImplementedException();
		}

		@Override
		public <T> T[] toArray(T[] arg0) {
			throw new NotImplementedException();
		}
	}

	class EntryIterator implements Iterator<Map.Entry<String, Map<K1, Map<K2, V>>>> {

		private final Iterator<String> keys;

		public EntryIterator(Iterator<String> keys) {
			this.keys = keys;
		}

		@Override
		public boolean hasNext() {
			return this.keys.hasNext();
		}

		@Override
		public java.util.Map.Entry<String, Map<K1, Map<K2, V>>> next() {
			final String key = this.keys.next();
			return new Map.Entry<String, Map<K1, Map<K2, V>>>() {
				@Override
				public String getKey() {
					return key;
				}

				@Override
				public Map<K1, Map<K2, V>> getValue() {
					return new SuperColumnFamilyKeyedMap<K1, K2, V>(SuperColumnFamilyMap.this.key1Type, SuperColumnFamilyMap.this.key2Type, SuperColumnFamilyMap.this.valType, client, SuperColumnFamilyMap.this.keyspace, SuperColumnFamilyMap.this.column_family, key, SuperColumnFamilyMap.this.readLevel, SuperColumnFamilyMap.this.writeLevel);
				}

				@Override
				public Map<K1, Map<K2, V>> setValue(Map<K1, Map<K2, V>> value) {
					throw new UnsupportedOperationException();
				}
			};
		}

		@Override
		public void remove() {
			this.keys.remove();
		}

	}
}

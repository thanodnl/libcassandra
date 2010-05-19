package nl.thanod.cassandra.alpha.map.supercolumn;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import nl.thanod.cassandra.alpha.bytes.NoTranslatorException;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Cassandra.Iface;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class SuperColumnFamilyKeyedMap<K1, K2, V> implements Map<K1, Map<K2, V>> {
	protected final Class<K1> key1Type;
	protected final Class<K2> key2Type;
	protected final Class<V> valType;

	protected final Iface client;
	protected final String keyspace;
	protected final String column_family;
	protected final String key;

	public SuperColumnFamilyKeyedMap(Class<K1> key1Type, Class<K2> key2Type, Class<V> valType, Cassandra.Iface client, String keyspace, String column_family, String key, ConsistencyLevel readLevel, ConsistencyLevel writeLevel) {
		this.key1Type = key1Type;
		this.key2Type = key2Type;
		this.valType = valType;

		this.client = client;
		this.keyspace = keyspace;
		this.column_family = column_family;
		this.key = key;
	}

	@Override
	public void clear() {
		Iterator<K1> keys = this.keySet().iterator();
		while (keys.hasNext()) {
			keys.next();
			keys.remove();
		}
	}

	@Override
	public boolean containsKey(Object arg0) {
		throw new NotImplementedException();
	}

	@Override
	public boolean containsValue(Object arg0) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<java.util.Map.Entry<K1, Map<K2, V>>> entrySet() {
		throw new NotImplementedException();
	}

	@Override
	public Map<K2, V> get(Object arg0) {
		throw new NotImplementedException();
	}

	@Override
	public boolean isEmpty() {
		throw new NotImplementedException();
	}

	@Override
	public Set<K1> keySet() {
		try {
			return new SuperColumnFamilyKeyedKeySet<K1>(key1Type, client, keyspace, column_family, key);
		} catch (NoTranslatorException ball) {
			throw new RuntimeException("Could not recover from " + ball.getClass().getCanonicalName(), ball);
		}
	}

	@Override
	public Map<K2, V> put(K1 arg0, Map<K2, V> arg1) {
		throw new NotImplementedException();
	}

	@Override
	public void putAll(Map<? extends K1, ? extends Map<K2, V>> maps) {
		throw new NotImplementedException();
	}

	@Override
	public Map<K2, V> remove(Object arg0) {
		throw new NotImplementedException();
	}

	@Override
	public int size() {
		throw new NotImplementedException();
	}

	@Override
	public Collection<Map<K2, V>> values() {
		throw new UnsupportedOperationException();
	}
}

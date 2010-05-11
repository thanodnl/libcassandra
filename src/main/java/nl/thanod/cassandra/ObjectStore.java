package nl.thanod.cassandra;

public interface ObjectStore<T> extends Iterable<T> {
	T load(byte [] key);
	void store(T object);
}

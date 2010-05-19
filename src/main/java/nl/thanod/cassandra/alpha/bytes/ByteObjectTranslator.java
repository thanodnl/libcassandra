package nl.thanod.cassandra.alpha.bytes;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.imageio.spi.ServiceRegistry;

public abstract class ByteObjectTranslator<T> {

	public static final Map<Class<?>, ByteObjectTranslator<?>> translatormap = new HashMap<Class<?>, ByteObjectTranslator<?>>();

	protected final Class<T> type;

	public ByteObjectTranslator(Class<T> type) {
		this.type = type;
	}

	public final Class<T> getType() {
		return type;
	}

	public abstract byte[] get(T o);

	public abstract T get(byte[] b);

	@SuppressWarnings("unchecked")
	public static <T> ByteObjectTranslator<T> getTranslatorFor(Class<T> type) throws NoTranslatorException {
		ByteObjectTranslator<?> t = translatormap.get(type);
		if (t == null){
			populateTranslatorMap();
			t = translatormap.get(type);
		}
		if (t == null)
			throw new NoTranslatorException(type);
		return (ByteObjectTranslator<T>)t;
	}

	@SuppressWarnings("unchecked")
	private static void populateTranslatorMap() {
		Iterator<ByteObjectTranslator> list = ServiceRegistry.lookupProviders(ByteObjectTranslator.class);
		while (list.hasNext()) {
			ByteObjectTranslator<?> t = list.next();
			translatormap.put(t.getClass(), t);
		}
	}
}

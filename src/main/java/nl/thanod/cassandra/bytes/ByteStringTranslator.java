package nl.thanod.cassandra.bytes;

import java.lang.reflect.Field;
import java.nio.charset.Charset;

import nl.thanod.annotations.spi.ProviderFor;

@ProviderFor(ByteTranslator.class)
public class ByteStringTranslator implements ByteTranslator {

	public static final Charset ENCODING = Charset.forName("UTF-8");

	@Override
	public boolean canTranslate(Class<?> type) {
		return String.class.equals(type);
	}

	@Override
	public byte[] getBytes(Field f, Object o) {
		Throwable thing = null;
		try {
			return bytes((String) f.get(o));
		} catch (Throwable ball) {
			thing = ball;
		}
		throw new RuntimeException("Unable to transform " + f.getDeclaringClass() + "." + f.getName() + " in to a " + byte[].class.getCanonicalName(), thing);
	}

	@Override
	public String getObject(byte[] bytes) {
		return make(bytes);
	}

	@Override
	public void setBytes(Field f, Object o, byte[] bytes) {
		Throwable thing = null;
		try {
			f.set(o, make(bytes));
			return;
		} catch (Throwable ball) {
			thing = ball;
		}
		throw new RuntimeException("Unable to set " + f.getDeclaringClass().getCanonicalName() + "." + f.getName(), thing);
	}

	public static byte[] bytes(String s) {
		return s.getBytes(ENCODING);
	}

	public static String make(byte[] bytes) {
		return new String(bytes, ENCODING);
	}

	@Override
	public byte[] getBytes(Object o) {
		return bytes((String)o);
	}

}

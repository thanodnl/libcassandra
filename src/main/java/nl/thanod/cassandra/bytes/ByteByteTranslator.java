package nl.thanod.cassandra.bytes;

import java.lang.reflect.Field;

import nl.thanod.annotations.spi.ProviderFor;

@ProviderFor(ByteTranslator.class)
public class ByteByteTranslator implements ByteTranslator {

	@Override
	public boolean canTranslate(Class<?> type) {
		return byte[].class.equals(type);
	}

	@Override
	public byte[] getBytes(Field f, Object o) {
		Throwable thing = null;
		try {
			return (byte[]) f.get(o);
		} catch (Throwable ball) {
			thing = ball;
		}
		throw new RuntimeException("Unable to transform " + f.getDeclaringClass() + "." + f.getName() + " in to a " + byte[].class.getCanonicalName(), thing);
	}

	@Override
	public byte[] getObject(byte[] bytes) {
		return bytes;
	}

	@Override
	public void setBytes(Field f, Object o, byte[] bytes) {
		Throwable thing = null;
		try {
			f.set(o, bytes);
			return;
		} catch (Throwable ball) {
			thing = ball;
		}
		throw new RuntimeException("Unable to set " + f.getDeclaringClass().getCanonicalName() + "." + f.getName(), thing);
	}

}

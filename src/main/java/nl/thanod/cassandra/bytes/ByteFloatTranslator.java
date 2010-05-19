package nl.thanod.cassandra.bytes;

import java.lang.reflect.Field;

import nl.thanod.annotations.spi.ProviderFor;

@ProviderFor(ByteTranslator.class)
public class ByteFloatTranslator implements ByteTranslator {

	@Override
	public boolean canTranslate(Class<?> type) {
		return float.class.equals(type);
	}

	@Override
	public byte[] getBytes(Field f, Object o) {
		Throwable thing = null;
		try {
			return bytes(f.getFloat(o));
		} catch (Throwable ball) {
			thing = ball;
		}
		throw new RuntimeException("Unable to transform " + f.getDeclaringClass() + "." + f.getName() + " in to a " + byte[].class.getCanonicalName(), thing);
	}

	@Override
	public Float getObject(byte[] bytes) {
		return make(bytes);
	}

	@Override
	public void setBytes(Field f, Object o, byte[] bytes) {
		Throwable thing = null;
		try {
			f.setFloat(o, make(bytes));
			return;
		} catch (Throwable ball) {
			thing = ball;
		}
		throw new RuntimeException("Unable to set " + f.getDeclaringClass().getCanonicalName() + "." + f.getName(), thing);
	}

	public static byte[] bytes(float f) {
		return ByteIntTranslator.bytes(Float.floatToIntBits(f));
	}

	public static float make(byte[] bytes) {
		return Float.intBitsToFloat(ByteIntTranslator.make(bytes));
	}

	@Override
	public byte[] getBytes(Object o) {
		return bytes((Float)o);
	}
}

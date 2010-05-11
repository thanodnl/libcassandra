package nl.thanod.cassandra.bytes;

import java.lang.reflect.Field;

import nl.thanod.annotations.spi.ProviderFor;

@ProviderFor(ByteTranslator.class)
public class ByteDoubleTranslator implements ByteTranslator {

	@Override
	public boolean canTranslate(Class<?> type) {
		return double.class.equals(type);
	}

	@Override
	public byte[] getBytes(Field f, Object o) {
		Throwable thing = null;
		try {
			return bytes(f.getDouble(o));
		} catch (Throwable ball) {
			thing = ball;
		}
		throw new RuntimeException("Unable to transform " + f.getDeclaringClass().getCanonicalName() + "." + f.getName() + " in to a " + byte[].class.getCanonicalName(), thing);
	}

	@Override
	public Double getObject(byte[] bytes) {
		return make(bytes);
	}

	@Override
	public void setBytes(Field f, Object o, byte[] bytes) {
		Throwable thing = null;
		try {
			f.setDouble(o, make(bytes));
			return;
		} catch (Throwable ball) {
			thing = ball;
		}
		throw new RuntimeException("Unable to set " + f.getDeclaringClass().getCanonicalName() + "." + f.getName(), thing);
	}

	public static byte[] bytes(double d) {
		return ByteStringTranslator.bytes(Double.toString(d));
	}

	public static double make(byte[] bytes) {
		return Double.parseDouble(ByteStringTranslator.make(bytes));
	}

}

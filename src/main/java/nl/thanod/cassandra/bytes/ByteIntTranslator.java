package nl.thanod.cassandra.bytes;

import java.lang.reflect.Field;

import nl.thanod.annotations.spi.ProviderFor;

@ProviderFor(ByteTranslator.class)
public class ByteIntTranslator implements ByteTranslator {

	@Override
	public boolean canTranslate(Class<?> type) {
		return int.class.equals(type);
	}

	@Override
	public byte[] getBytes(Field f, Object o) {
		Throwable thing = null;
		try {
			return bytes(f.getInt(o));
		} catch (Throwable ball) {
			thing = ball;
		}
		throw new RuntimeException("Unable to transform " + f.getDeclaringClass() + "." + f.getName() + " in to a " + byte[].class.getCanonicalName(), thing);
	}

	@Override
	public Integer getObject(byte[] bytes) {
		return make(bytes);
	}
	
	@Override
	public void setBytes(Field f, Object o, byte[] bytes) {
		Throwable thing = null;
		try {
			f.setInt(o, make(bytes));
			return;
		} catch (Throwable ball) {
			thing = ball;
		}
		throw new RuntimeException("Unable to set " + f.getDeclaringClass().getCanonicalName() + "." + f.getName(), thing);
	}

	public static byte[] bytes(int i) {
		byte[] b = new byte[4];
		b[0] = (byte) ((i >> 24) & 0xFF);
		b[1] = (byte) ((i >> 16) & 0xFF);
		b[2] = (byte) ((i >> 8) & 0xFF);
		b[3] = (byte) ((i >> 0) & 0xFF);
		return b;
	}
	
	public static int make(byte[] bytes) {
		if (bytes.length != 4)
			throw new RuntimeException("Expected 4 bytes instead of " + bytes.length);
		int i = 0;
		i |= (bytes[0] & 0xFF) << 24;
		i |= (bytes[1] & 0xFF) << 16;
		i |= (bytes[2] & 0xFF) << 8;
		i |= (bytes[3] & 0xFF) << 0;
		return i;
	}


}

package nl.thanod.cassandra.bytes;

import java.lang.reflect.Field;
import java.util.Arrays;

import nl.thanod.annotations.spi.ProviderFor;

@ProviderFor(ByteTranslator.class)
public class ByteLongTranslator implements ByteTranslator {

	@Override
	public boolean canTranslate(Class<?> type) {
		return long.class.equals(type);
	}

	@Override
	public byte[] getBytes(Field f, Object o) {
		Throwable thing = null;
		try {
			return bytes(f.getLong(o));
		} catch (Throwable ball) {
			thing = ball;
		}
		throw new RuntimeException("Unable to transform " + f.getDeclaringClass() + "." + f.getName() + " in to a " + byte[].class.getCanonicalName(), thing);
	}

	@Override
	public Long getObject(byte[] bytes) {
		return make(bytes);
	}

	@Override
	public void setBytes(Field f, Object o, byte[] bytes) {
		Throwable thing = null;
		try {
			f.setLong(o, make(bytes));
			return;
		} catch (Throwable ball) {
			thing = ball;
		}
		throw new RuntimeException("Unable to set " + f.getDeclaringClass().getCanonicalName() + "." + f.getName(), thing);
	}

	public static byte[] bytes(long l) {
		byte[] b = new byte[8];
		 b[0] = (byte) ((l >> 56) & 0xFF);
		 b[1] = (byte) ((l >> 48) & 0xFF);
		 b[2] = (byte) ((l >> 40) & 0xFF);
		 b[3] = (byte) ((l >> 32) & 0xFF);
		 b[4] = (byte) ((l >> 24) & 0xFF);
		 b[5] = (byte) ((l >> 16) & 0xFF);
		 b[6] = (byte) ((l >> 8) & 0xFF);
		 b[7] = (byte) ((l >> 0) & 0xFF);
		return b;
	}

	public static long make(byte[] bytes) {
		if (bytes.length != 8)
			throw new RuntimeException("Expected 8 bytes instead of " + bytes.length);
		long l = 0;
		l |= (long)(bytes[0] & 0xFF) << 56;
		l |= (long)(bytes[1] & 0xFF) << 48;
		l |= (long)(bytes[2] & 0xFF) << 40;
		l |= (long)(bytes[3] & 0xFF) << 32;
		l |= (long)(bytes[4] & 0xFF) << 24;
		l |= (long)(bytes[5] & 0xFF) << 16;
		l |= (long)(bytes[6] & 0xFF) << 8;
		l |= (long)(bytes[7] & 0xFF) << 0;
		return l;
	}
}

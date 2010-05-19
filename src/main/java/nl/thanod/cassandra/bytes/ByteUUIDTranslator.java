package nl.thanod.cassandra.bytes;

import java.lang.reflect.Field;
import java.util.UUID;

import nl.thanod.annotations.spi.ProviderFor;

@ProviderFor(ByteTranslator.class)
public class ByteUUIDTranslator implements ByteTranslator {

	@Override
	public boolean canTranslate(Class<?> type) {
		return UUID.class.equals(type);
	}

	@Override
	public byte[] getBytes(Field f, Object o) {
		try {
			UUID uuid = (UUID) f.get(o);
			return bytes(uuid);
		} catch (Throwable ball) {
			return null;
		}
	}

	@Override
	public Object getObject(byte[] bytes) {
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

	public static byte[] bytes(UUID uuid) {
		long msb = uuid.getMostSignificantBits();
		long lsb = uuid.getLeastSignificantBits();
		byte[] buffer = new byte[16];
		for (int i = 0; i < 8; i++)
			buffer[i] = (byte) (msb >>> 8 * (7 - i));
		for (int i = 8; i < 16; i++)
			buffer[i] = (byte) (lsb >>> 8 * (7 - i));
		return buffer;
	}

	public static UUID make(byte[] bytes) {
		long msb = 0;
		long lsb = 0;
		assert bytes.length == 16;
		for (int i = 0; i < 8; i++)
			msb = (msb << 8) | (bytes[i] & 0xff);
		for (int i = 8; i < 16; i++)
			lsb = (lsb << 8) | (bytes[i] & 0xff);
		return new UUID(msb, lsb);
	}

	@Override
	public byte[] getBytes(Object o) {
		return bytes((UUID)o);
	}
}

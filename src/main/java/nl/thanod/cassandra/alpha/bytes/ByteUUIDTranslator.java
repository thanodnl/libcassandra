package nl.thanod.cassandra.alpha.bytes;

import java.util.UUID;

import nl.thanod.annotations.spi.ProviderFor;

@ProviderFor(ByteObjectTranslator.class)
public class ByteUUIDTranslator extends ByteObjectTranslator<UUID> {

	public ByteUUIDTranslator() {
		super(UUID.class);
	}

	@Override
	public byte[] get(UUID o) {
		long msb = o.getMostSignificantBits();
		long lsb = o.getLeastSignificantBits();
		byte[] buffer = new byte[16];
		for (int i = 0; i < 8; i++)
			buffer[i] = (byte) (msb >>> 8 * (7 - i));
		for (int i = 8; i < 16; i++)
			buffer[i] = (byte) (lsb >>> 8 * (7 - i));
		return buffer;
	}

	@Override
	public UUID get(byte[] b) {
		if (b.length != 16)
			throw new NumberFormatException("An UUID is always 16 bytes long, not " + b.length);
		long msb = 0;
		long lsb = 0;
		for (int i = 0; i < 8; i++)
			msb = (msb << 8) | (b[i] & 0xff);
		for (int i = 8; i < 16; i++)
			lsb = (lsb << 8) | (b[i] & 0xff);
		return new UUID(msb, lsb);
	}

}

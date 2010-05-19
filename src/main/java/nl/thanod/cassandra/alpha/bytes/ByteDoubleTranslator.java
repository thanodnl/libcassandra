package nl.thanod.cassandra.alpha.bytes;

import nl.thanod.annotations.spi.ProviderFor;

@ProviderFor(ByteObjectTranslator.class)
public class ByteDoubleTranslator extends ByteObjectTranslator<Double> {

	public ByteDoubleTranslator() {
		super(Double.class);
	}

	@Override
	public byte[] get(Double o) {
		byte[] bytes = new byte[8];
		long l = Double.doubleToLongBits(o);
		bytes[7] = (byte) ((l >> 56) & 0xFF);
		bytes[6] = (byte) ((l >> 48) & 0xFF);
		bytes[5] = (byte) ((l >> 40) & 0xFF);
		bytes[4] = (byte) ((l >> 32) & 0xFF);
		bytes[3] = (byte) ((l >> 24) & 0xFF);
		bytes[2] = (byte) ((l >> 16) & 0xFF);
		bytes[1] = (byte) ((l >> 8) & 0xFF);
		bytes[0] = (byte) ((l >> 0) & 0xFF);
		return bytes;
	}

	@Override
	public Double get(byte[] b) {
		if (b.length != 8)
			throw new NumberFormatException("A double is always 8 bytes, not " + b.length);
		long l = 0;
		l |= (long) (b[7] & 0xFF) << 56;
		l |= (long) (b[6] & 0xFF) << 48;
		l |= (long) (b[5] & 0xFF) << 40;
		l |= (long) (b[4] & 0xFF) << 32;
		l |= (long) (b[3] & 0xFF) << 24;
		l |= (long) (b[2] & 0xFF) << 16;
		l |= (long) (b[1] & 0xFF) << 8;
		l |= (long) (b[0] & 0xFF) << 0;
		return Double.longBitsToDouble(l);
	}

}

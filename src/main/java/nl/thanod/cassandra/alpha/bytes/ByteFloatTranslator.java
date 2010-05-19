package nl.thanod.cassandra.alpha.bytes;

import nl.thanod.annotations.spi.ProviderFor;

@ProviderFor(ByteObjectTranslator.class)
public class ByteFloatTranslator extends ByteObjectTranslator<Float> {

	public ByteFloatTranslator() {
		super(Float.class);
	}

	@Override
	public byte[] get(Float o) {
		byte [] bytes = new byte [ 4];
		int i = Float.floatToIntBits(o);
		bytes[3] = (byte)((i >> 24) & 0xFF);
		bytes[2] = (byte)((i >> 16) & 0xFF);
		bytes[1] = (byte)((i >> 8) & 0xFF);
		bytes[0] = (byte)((i >> 0) & 0xFF);
		return bytes;
	}

	@Override
	public Float get(byte[] b) {
		if (b.length != 4)
			throw new NumberFormatException("An integer is always 4 bytes, not " + b.length);
		int i = 0;
		i |= ((int)b[3]) << 24;
		i |= ((int)b[2]) << 16;
		i |= ((int)b[1]) << 8;
		i |= ((int)b[0]) << 0;
		return Float.intBitsToFloat(i);
	}

}

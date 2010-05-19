package nl.thanod.cassandra.alpha.bytes;

import nl.thanod.annotations.spi.ProviderFor;
import java.nio.charset.Charset;

@ProviderFor(ByteObjectTranslator.class)
public class ByteStringTranslator extends ByteObjectTranslator<String> {
	public static final Charset ENCODING = Charset.forName("UTF-8");

	public ByteStringTranslator() {
		super(String.class);
	}

	@Override
	public byte[] get(String o) {
		return o.getBytes(ByteStringTranslator.ENCODING);
	}

	@Override
	public String get(byte[] b) {
		return new String(b, ByteStringTranslator.ENCODING);
	}
}

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import nl.thanod.cassandra.bytes.ByteLongTranslator;


public class Test {
	public static void main(String... args) throws IOException {
		long l = 1273354838512L;
		System.out.println(Arrays.toString(ByteLongTranslator.bytes(l)));
		DataOutputStream dos = new DataOutputStream(new PrintingOutputStream());
		dos.writeLong(l);
	}
	
	static class PrintingOutputStream extends OutputStream {
		
		
		@Override
		public void write(int paramInt) throws IOException {
			System.out.println(paramInt);
		}
		
	}
}

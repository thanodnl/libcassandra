package nl.thanod.cassandra.bytes;

import java.lang.reflect.Field;

public interface ByteTranslator {
	boolean canTranslate(Class<?> type);

	byte[] getBytes(Field f, Object o);
	void setBytes(Field f, Object o, byte [] bytes);
	
	Object getObject(byte[] bytes);
}

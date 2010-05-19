package nl.thanod.cassandra;

import java.io.InvalidClassException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.util.*;

import javax.imageio.spi.ServiceRegistry;

import nl.thanod.cassandra.bytes.ByteStringTranslator;
import nl.thanod.cassandra.bytes.ByteTranslator;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.TException;

public class Store {
	public static final Charset STRING_ENCODING = Charset.forName("UTF-8");

	public static <T> T ColumnsToObject(Class<T> clazz, Iterable<Column> columns, byte[] key) throws InstantiationException, IllegalAccessException, InvalidClassException, SecurityException, NoSuchMethodException, IllegalArgumentException, InvocationTargetException {
		if (columns == null)
			return null;
		Constructor<T> con = clazz.getDeclaredConstructor();
		if (con == null)
			return null;
		if (!con.isAccessible())
			con.setAccessible(true);
		T o = con.newInstance();
//		T o = clazz.newInstance();
		for (Field f : clazz.getDeclaredFields()) {
			if ((f.getModifiers() & Modifier.TRANSIENT) == Modifier.TRANSIENT)
				continue;
			Column c = getColumnByName(columns, ByteStringTranslator.bytes(f.getName()));
			// check if there is a column found for this field
			if (c == null)
				throw new InvalidClassException("Could not find an entry for the non-transient field " + f.getDeclaringClass().getCanonicalName() + "." + f.getName() + " in the database.");
			make(f, o, c.value);
		}

		Field keyfield = getKeyField(clazz);
		if (keyfield != null)
			make(keyfield, o, key);
		return o;
	}

	public static String getFieldFullName(Field f) {
		return f.getDeclaringClass() + "." + f.getName();
	}

	public static <T> T load(Client client, String keyspace, String column_family, byte[] key, Class<T> type) {
		try {
			ColumnParent parent = new ColumnParent(column_family);
			SlicePredicate predicate = new SlicePredicate();
			predicate.column_names = new LinkedList<byte[]>();
			for (Field f : type.getDeclaredFields())
				if ((f.getModifiers() & Modifier.TRANSIENT) != Modifier.TRANSIENT)
					predicate.column_names.add(ByteStringTranslator.bytes(f.getName()));
			List<ColumnOrSuperColumn> loaded = client.get_slice(keyspace, ByteStringTranslator.make(key), parent, predicate, ConsistencyLevel.ONE);
			List<Column> columns = new LinkedList<Column>();
			for (ColumnOrSuperColumn c : loaded)
				columns.add(c.column);
			return ColumnsToObject(type, columns, key);
		} catch (Throwable ball) {
			ball.printStackTrace();
			return null;
		}
	}

	public static <T> T load(Client client, String keyspace, String column_fammily, String key, byte[] super_column, Class<T> type) {

		Throwable thing = null;
		try {
			ColumnPath path = new ColumnPath();
			path.column_family = column_fammily;
			if (super_column != null)
				path.super_column = super_column;

			ColumnOrSuperColumn corsc = client.get(keyspace, key, path, ConsistencyLevel.ONE);
			if (corsc.super_column == null)
				throw new InvalidClassException("Could not find a " + type.getCanonicalName() + " at " + keyspace + "." + column_fammily + "['" + key + "'][" + Arrays.toString(super_column) + "]");
			return ColumnsToObject(type, corsc.super_column.columns, corsc.super_column.name);
		} catch (NotFoundException ball) {
			return null;
		} catch (Throwable ball) {
			thing = ball;
		}
		throw new RuntimeException("Was not able to load an instance of " + type.getCanonicalName(), thing);
	}

	public static <T> T load(SuperColumn super_column, Class<T> type) throws InvalidClassException, InstantiationException, IllegalAccessException, SecurityException, IllegalArgumentException, NoSuchMethodException, InvocationTargetException {
		return ColumnsToObject(type, super_column.columns, super_column.name);
	}

	public static List<Column> ObjectToColumn(Object o) {
		long time = getCassandraTime();
		List<Column> list = new LinkedList<Column>();
		Class<?> type = o.getClass();
		for (Field f : type.getDeclaredFields()) {
			if ((f.getModifiers() & Modifier.TRANSIENT) == Modifier.TRANSIENT)
				continue;
			if (!f.isAccessible())
				f.setAccessible(true);
			byte[] val = bytes(o, f);
			if (val != null)
				list.add(new Column(ByteStringTranslator.bytes(f.getName()), val, time));
		}
		return list;
	}

//	public static void store(Cassandra.Client client, String keyspace, String column_family, ConsistencyLevel consistency_level, Object... os) throws InvalidRequestException, UnavailableException, TimedOutException, TException {
//		for (Object o : os) {
//			Class<?> clazz = o.getClass();
//			Field key = getKeyField(clazz);
//			List<Column> columns = ObjectToColumn(o);
//			if (key == null)
//				throw new RuntimeException("No key defined in " + clazz.getCanonicalName());
//			if (!key.isAccessible())
//				key.setAccessible(true);
//			// if (key.getType().equals(String.class))
//			// throw new
//			// RuntimeException("Due to limits of the current cassandra thrift api the key "
//			// + getFieldFullName(key) + " has to be from type " +
//			// String.class);
//			try {
//				Map<String, List<ColumnOrSuperColumn>> cfmap = new TreeMap<String, List<ColumnOrSuperColumn>>();
//				String k;
//				if (key.getType().equals(String.class))
//					k = (String) key.get(o);
//				else{
//					k = ByteStringTranslator.make(bytes(o, key));
//				}
//				List<ColumnOrSuperColumn> list = cfmap.get(k);
//				if (list == null) {
//					list = new LinkedList<ColumnOrSuperColumn>();
//					cfmap.put(column_family, list);
//				}
//				for (Column co : columns) {
//					ColumnOrSuperColumn c = new ColumnOrSuperColumn();
//					c.column = co;
//					list.add(c);
//				}
//				client.batch_insert(keyspace, k, cfmap, consistency_level);
//			} catch (Throwable ball) {
//				throw new RuntimeException(ball);
//			}
//		}
//	}

	public static void store(Cassandra.Client client, String keyspace, String column_family, String super_column, ConsistencyLevel consistency_level, Object... os) throws InvalidRequestException, UnavailableException, TimedOutException, TException {
		Map<String, List<ColumnOrSuperColumn>> cfmap = new TreeMap<String, List<ColumnOrSuperColumn>>();
		for (Object o : os) {
			Class<?> clazz = o.getClass();
			Field key = getKeyField(clazz);
			List<Column> columns = ObjectToColumn(o);
			if (key == null)
				throw new RuntimeException("No key defined in " + clazz.getCanonicalName());
			if (!key.isAccessible())
				key.setAccessible(true);

			ColumnOrSuperColumn c = new ColumnOrSuperColumn();
			SuperColumn s = new SuperColumn();
			s.columns = columns;
			s.name = bytes(o, key);
			c.super_column = s;

			List<ColumnOrSuperColumn> list = cfmap.get(column_family);
			if (list == null) {
				list = new LinkedList<ColumnOrSuperColumn>();
				cfmap.put(column_family, list);
			}
			list.add(c);
		}
		client.batch_insert(keyspace, super_column, cfmap, consistency_level);
	}

	private static byte[] bytes(Object o, Field f) {
		Class<?> type = f.getType();
		Iterator<ByteTranslator> list = ServiceRegistry.lookupProviders(ByteTranslator.class);
		while (list.hasNext()) {
			ByteTranslator trans = list.next();
			if (trans.canTranslate(type))
				return trans.getBytes(f, o);
		}
		throw new RuntimeException("Not able to store " + getFieldFullName(f) + " because it is not known how a " + type.getCanonicalName() + " is transformed in a " + byte[].class.getCanonicalName());
	}

	private static long getCassandraTime() {
		return System.currentTimeMillis() * 1000;
	}

	private static Column getColumnByName(Iterable<Column> columns, byte[] key) {
		for (Column c : columns)
			if (Arrays.equals(key, c.name))
				return c;
		return null;
	}

	private static Field getKeyField(Class<?> clazz) {
		Field key = null;
		for (Field f : clazz.getDeclaredFields())
			if (f.isAnnotationPresent(Key.class))
				if (key == null)
					key = f;
				else
					throw new RuntimeException("There are multiple keys defined in " + clazz.getCanonicalName() + ". For example but not limited to " + getFieldFullName(key) + " and " + getFieldFullName(key));
		return key;
	}

	private static void make(Field f, Object o, byte[] bytes) {
		Class<?> type = f.getType();
		Iterator<ByteTranslator> list = ServiceRegistry.lookupProviders(ByteTranslator.class);
		while (list.hasNext()) {
			ByteTranslator trans = list.next();
			if (trans.canTranslate(type)) {
				if (!f.isAccessible())
					f.setAccessible(true);
				trans.setBytes(f, o, bytes);
				return;
			}
		}
		throw new RuntimeException("Not able to restore " + getFieldFullName(f) + " because it is not known how a " + byte[].class.getCanonicalName() + " is transformed in a " + type.getCanonicalName());
	}
}

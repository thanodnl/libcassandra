package nl.thanod;

import java.util.Iterator;
import java.util.Set;

import nl.thanod.cassandra.alpha.map.supercolumn.SuperColumnFamilyKeySet;

import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class MapperTest {
	public static final byte [] EMPTY = new byte[0];
	
	public static void main(String... args) throws InvalidRequestException, UnavailableException, TimedOutException, TException {
		TTransport tr = new TSocket("localhost", 9160);
		TProtocol proto = new TBinaryProtocol(tr);
		Cassandra.Client client = new Cassandra.Client(proto);
		tr.open();
		
		ColumnParent column_parent;
		SlicePredicate predicate;
		long start,took,c;
		
//		start = System.currentTimeMillis();
//		SuperColumnFamilyMap<?,?,?> maps = new SuperColumnFamilyMap<Object,Object,Object>(Object.class,Object.class,Object.class,client,"gamelink","PlayerSessions",ConsistencyLevel.ONE,ConsistencyLevel.ZERO);
//		maps.clear();
//		took = System.currentTimeMillis() - start;
//		System.out.println("took " + took + "ms");

		start = System.currentTimeMillis();
		Set<String> keys = new SuperColumnFamilyKeySet(client, "gamelink", "PlayerSessions");
		Iterator<String> kit = keys.iterator();
		if (kit.hasNext()){
			kit.next();
			kit.remove();
		}
		
//		c = 0;
//		for (String s : keys){
//			c++;
//			System.out.println(s);
//		}
		took = System.currentTimeMillis() - start;
		System.out.println("took " + took + "ms");	
//		System.out.println("found " + c + "items");
		
//		column_parent = new ColumnParent("PlayerSessions");
//		predicate = new SlicePredicate();
//		predicate.slice_range = new SliceRange(EMPTY, EMPTY, false, 100);
//		for (ColumnOrSuperColumn col:client.get_slice("gamelink", "ThaNODnl", column_parent, predicate, ConsistencyLevel.ONE)){
//			System.out.println(col);
//		}
		
		
//		start = System.currentTimeMillis();
//		column_parent = new ColumnParent("PlayerSessions");
//		predicate = new SlicePredicate();
//		predicate.slice_range = new SliceRange(EMPTY, EMPTY, false, 1);
//		for (KeySlice k:client.get_range_slice("gamelink", column_parent, predicate, "Player14405", "", 5, ConsistencyLevel.ANY)){
//			System.out.println(k);
//		}
//		took = System.currentTimeMillis() - start;
//		System.out.println("took " + took + "ms");
	}
}

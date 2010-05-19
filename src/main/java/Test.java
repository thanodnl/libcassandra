import java.util.Map;
import java.util.UUID;

import nl.thanod.TimedUUIDGenerator;
import nl.thanod.cassandra.alpha.map.ColumnFamilyMap;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class Test {
	public static void main(String... args) throws TTransportException {
		TTransport tr = new TSocket("localhost", 9160);
		TProtocol proto = new TBinaryProtocol(tr);
		Cassandra.Client client = new Cassandra.Client(proto);
		tr.open();
		Map<UUID, String> playerSessions = new ColumnFamilyMap<UUID, String>(UUID.class, String.class, client, "gamelink", "Sessions", "Players", ConsistencyLevel.ONE);
		long start, took;
		//				start = System.currentTimeMillis();
		//				playerSessions.clear();
		//				took = System.currentTimeMillis() - start;
		//				System.out.println("took " + took + "ms");
		for (int i = 0; i < 100; i++) {
			start = System.currentTimeMillis();
			System.out.println(playerSessions.size());
			took = System.currentTimeMillis() - start;
			System.out.println("took " + took + "ms");
		}

		//		List<UUID> uuids = new LinkedList<UUID>();
		//		for (int i = 0; i < 100; i++) {
		//			start = System.currentTimeMillis();
		//			for (UUID uuid : uuids)
		//				System.out.println(uuid + ": " + playerSessions.get(uuid));
		//			took = System.currentTimeMillis() - start;
		//			System.out.println("took " + took + "ms");
		//		}

		//		for (Map.Entry<UUID, String> e:playerSessions.entrySet()){
		//			System.out.println(e.getKey() + ": " + e.getValue());
		//		}

		//		start = System.currentTimeMillis();
		//		String[] players = { "ThaNODnl", "specialist_nl", "killerman", "crucher", "falcon", "maller" };
		//		for (int i = 0; i < 1000000; i++)
		//			playerSessions.put(TimedUUIDGenerator.getTimeBasedUUID(), players[i % players.length]);
		//		took = System.currentTimeMillis() - start;
		//		System.out.println("took " + took + "ms");
		//		
		//		start = System.currentTimeMillis();
		//		System.out.println(playerSessions.size());
		//		took = System.currentTimeMillis() - start;
		//		System.out.println("took " + took + "ms");
	}
}

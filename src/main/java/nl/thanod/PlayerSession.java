package nl.thanod;

import java.io.InvalidClassException;
import java.util.List;

import nl.thanod.cassandra.Key;

import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class PlayerSession {
	@Key
	private transient final long key;
	private String firstseen;
	private String lastseen;
	private final String server;
	private final String team;

	@Override
	public String toString() {
		return "PlayerSession [key=" + key + ", firstseen=" + firstseen + ", lastseen=" + lastseen + ", server=" + server + ", team=" + team + "]";
	}

	public PlayerSession() {
		this(null, null);
	}

	public PlayerSession(String server, String team) {
		this.key = System.currentTimeMillis();
		this.lastseen = this.firstseen = Long.toString(this.key);
		this.server = server;
		this.team = team;
	}

	public void seen() {
		this.lastseen = Long.toString(System.currentTimeMillis());
	}

	// @Override
	// public byte[] getKey() {
	// return Store.bytes(this.id);
	// }

	public static void main(String... args) throws InvalidRequestException, UnavailableException, TimedOutException, TException, InterruptedException, InvalidClassException, InstantiationException, IllegalAccessException, NotFoundException {
		TTransport tr = new TSocket("localhost", 9160);
		TProtocol proto = new TBinaryProtocol(tr);
		Cassandra.Client client = new Cassandra.Client(proto);
		tr.open();
		
		ColumnParent parent= new ColumnParent("OnlinePlayers");
		SlicePredicate predicate = new SlicePredicate();
		predicate.slice_range = new SliceRange(new byte[0], new byte[0], false, 100);
		List<ColumnOrSuperColumn> columns = client.get_slice("gamelink", "specialist_nl", parent, predicate, ConsistencyLevel.ONE);
		System.out.println(columns);
//		long start, took;
//		start = System.currentTimeMillis();
//		ObjectStore<PlayerSession> sessionsThaNODnl = new SuperColumnObjectStore<PlayerSession>(client, "gamelink", "PlayerSessions", "ThaNODnl", PlayerSession.class);
//		for (PlayerSession s : sessionsThaNODnl) {
//			System.out.println(s);
//		}
//		took = System.currentTimeMillis() - start;
//		System.out.println("took: " + took);
		
//		System.out.println(sessionsThaNODnl.load(ByteLongTranslator.bytes(12345)));
		
//		System.out.println(Store.load(client, "gamelink", "PlayerSessions", "ThaNODnl", ByteLongTranslator.bytes(1273354838512L), PlayerSession.class));

//		PlayerSession t1 = new PlayerSession("bc.mybad.nl:48801", "1");
//		System.out.println(t1);
//		List<Column> tocas = Store.ObjectToColumn(t1);
//		PlayerSession t2 = Store.ColumnsToObject(PlayerSession.class, tocas, new byte[0]);
//		System.out.println(t2);

//		for (int i = 0; i < 50; i++) {
//			PlayerSession t = new PlayerSession("bc.mybad.nl:48801", "" + i);
//			Store.store(client, "gamelink", "PlayerSessions", "ThaNODnl", ConsistencyLevel.ONE, t);
//			Thread.sleep(1000);
//			t.seen();
//		}

	}

}

package nl.thanod;

import java.io.InvalidClassException;
import java.util.UUID;

import nl.thanod.cassandra.Key;
import nl.thanod.cassandra.ObjectStore;
import nl.thanod.cassandra.Store;
import nl.thanod.cassandra.SuperColumnObjectStore;
import nl.thanod.cassandra.bytes.ByteUUIDTranslator;

import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class PlayerSession {
	@Key
	private transient final UUID key;
	private String firstseen;
	private String lastseen;
	private final String server;
	private final UUID server_session;
	private final String team;

	@Override
	public String toString() {
		return "PlayerSession [key=" + key + ", firstseen=" + firstseen + ", lastseen=" + lastseen + ", server=" + server + ", team=" + team + "]";
	}

	public PlayerSession() {
		this(null, null);
	}

	public PlayerSession(String server, String team) {
		this.key = TimedUUIDGenerator.getTimeBasedUUID();
		this.server_session = TimedUUIDGenerator.getTimeBasedUUID();
		this.lastseen = this.firstseen = Long.toString(System.currentTimeMillis());
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

		PlayerSession t = new PlayerSession("bc.mybad.nl:48801", "1");
		for (int i = 1000; i < 20000; i++)
			Store.store(client, "gamelink", "PlayerSessions", "Player" + i, ConsistencyLevel.ZERO, t);

		ObjectStore<PlayerSession> thanod = new SuperColumnObjectStore<PlayerSession>(client, "gamelink", "PlayerSessions", "ThaNODnl", PlayerSession.class);

		//		PlayerSession t = new PlayerSession("bc.mybad.nl:48801","1");
		//		thanod.store(t);

		//		UUID uuid = UUID.fromString("5a7214a0-61cd-11df-8e45-00236c001b40");
		//		System.out.println(thanod.load(ByteUUIDTranslator.bytes(uuid)));
//		for (PlayerSession p : thanod) {
//			System.out.println(p);
//		}
	}

}

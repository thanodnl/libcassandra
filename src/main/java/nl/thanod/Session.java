package nl.thanod;

import java.util.UUID;

import org.apache.cassandra.thrift.*;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import nl.thanod.cassandra.ColumnFamilyObjectStore;
import nl.thanod.cassandra.Key;
import nl.thanod.cassandra.ObjectStore;
import nl.thanod.cassandra.bytes.ByteStringTranslator;

public class Session {
	private final UUID uuid;
	
	@Key
	private final String player;
	
	private Session(){
		this.uuid = null;
		this.player = null;
	
	}
	
	public Session(String player, String server){
		this.uuid = TimedUUIDGenerator.getTimeBasedUUID();
		this.player = player;
	}
	
	
	public static void main(String... args) throws TTransportException {
		TTransport tr = new TSocket("localhost", 9160);
		TProtocol proto = new TBinaryProtocol(tr);
		Cassandra.Client client = new Cassandra.Client(proto);
		tr.open();
		
		ObjectStore<Session> sessions = new ColumnFamilyObjectStore<Session>(client, "gamelink", "Sessions", Session.class);
/*/		
		Session s = new Session("ThaNODnl","bc.mybad.nl:48801");
		sessions.store(s);
/*/	
		Session s = sessions.load(ByteStringTranslator.bytes("ThaNODnl"));
//*/
		System.out.println(s);
	}

	@Override
	public String toString() {
		return "Session [player=" + player + ", uuid=" + uuid + "]";
	}
}

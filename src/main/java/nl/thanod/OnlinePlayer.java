package nl.thanod;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import nl.thanod.cassandra.ColumnFamilyObjectStore;
import nl.thanod.cassandra.Key;
import nl.thanod.cassandra.ObjectStore;
import nl.thanod.cassandra.bytes.ByteStringTranslator;

public class OnlinePlayer {
	@Key
	private transient final String player;
	private final String server;
	private final long firstseen;
	private long lastseen;
	
	private OnlinePlayer(){
		this.player = null;
		this.server = null;
		this.firstseen = 0;
		this.lastseen = 0;
	}
	
	public OnlinePlayer(String player, String server){
		this.player = player;
		this.server = server;
		this.lastseen = this.firstseen = System.currentTimeMillis();
	}
	
	public void seen(){
		this.lastseen = System.currentTimeMillis();
	}

	@Override
	public String toString() {
		return "OnlinePlayer [player=" + player + ", server=" + server + ", firstseen=" + firstseen + ", lastseen=" + lastseen + "]";
	}
	
	public static void main(String... args) throws TTransportException {
		TTransport tr = new TSocket("localhost", 9160);
		TProtocol proto = new TBinaryProtocol(tr);
		Cassandra.Client client = new Cassandra.Client(proto);
		tr.open();
		
		ObjectStore<OnlinePlayer> onlinePlayers = new ColumnFamilyObjectStore<OnlinePlayer>(client, "gamelink", "OnlinePlayers", OnlinePlayer.class);

//		OnlinePlayer p = new OnlinePlayer("ThaNODnl","bc.mybad.nl:48801");
//		onlinePlayers.store(p);
		System.out.println(onlinePlayers.load(ByteStringTranslator.bytes("ThaNODnl")));
	}
}

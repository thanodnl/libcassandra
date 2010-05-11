package nl.thanod;

import nl.thanod.cassandra.Key;

public class OnlinePlayer {
	@Key
	private transient final String player;
	private final String server;
	private final long firstseen;
	private long lastseen;
	
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
	
	public static void main(String... args) {
		OnlinePlayer player = new OnlinePlayer("ThaNODnl","bc.mybad.nl:48801");
		System.out.println(player);
	}
}

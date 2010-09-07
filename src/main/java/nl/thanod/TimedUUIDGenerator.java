package nl.thanod;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.*;

public class TimedUUIDGenerator {
	public static final Comparator<byte[]> MAC_COMPARATOR = new Comparator<byte[]>() {
		@Override
		public int compare(byte[] o1, byte[] o2) {
			if (o1 == null)
				return -1;
			if (o2 == null)
				return 1;

			if (isLocalMAC(o1) != isLocalMAC(o2))
				return isLocalMAC(o1) ? -1 : 1;
			int len = Math.min(o1.length, o2.length);
			for (int i = 0; i < len; i++) {
				int diff = (o1[i] & 0xFF) - (o2[i] & 0xFF);
				if (diff != 0)
					return diff;
			}
			return o1.length - o2.length;
		}
	};

	public static final int CLOCK_ID_MASK = 0xFFF;
	public static final long TIME_MASK = 0x0FFFFFFFFFFFFFFFL;
	public static final long VERSION_MASK = 0x1000000000000000L;

	private static long CLOCK_ID;
	private static long LAST_TIME = 0;
	private static final Random RANDY = new Random(System.currentTimeMillis());
	private static long TIME_INCREMENT = 0;
	private static final byte[] USING_MAC;

	static {
		byte[] mac = null;
		try {
			Enumeration<NetworkInterface> inf = NetworkInterface.getNetworkInterfaces();
			while (inf.hasMoreElements()) {
				NetworkInterface n = inf.nextElement();
				byte[] m = n.getHardwareAddress();
				if (MAC_COMPARATOR.compare(mac, m) < 0)
					mac = m;
			}
		} catch (SocketException ball) {
		}
		if (mac == null || (isLocalMAC(mac) && !isGeneratedMacStrong(mac)))
			mac = generateMac();
		USING_MAC = mac;
		CLOCK_ID = RANDY.nextLong() & CLOCK_ID_MASK;
	}

	public static byte[] generateMac() {
		byte[] mac = new byte[6];
		RANDY.nextBytes(mac);
		mac[0] = 0x02;
		return mac;
	}

	public static String getMacString(byte[] b) {
		if (b == null)
			return null;
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < b.length; i++) {
			if (sb.length() > 0)
				sb.append(':');
			sb.append(Integer.toString((b[i] & 0xff) + 0x100, 16).substring(1));
		}
		return sb.toString();
	}

	public synchronized static UUID getTimeBasedUUID() {
		// convert the time to nanoseconds since 15 October 1582
		long uppertime = System.currentTimeMillis();// +Calendar.getInstance().getTimeZone().getOffset(System.currentTimeMillis());
		uppertime = uppertime * 10000 + 122192928000000000L;
		uppertime &= TIME_MASK;
		if (uppertime < LAST_TIME)
			CLOCK_ID = (CLOCK_ID + 1) & CLOCK_ID_MASK;
		if (uppertime == LAST_TIME)
			TIME_INCREMENT++;
		else
			TIME_INCREMENT = 0;
		LAST_TIME = uppertime;

		uppertime += TIME_INCREMENT;
		uppertime &= TIME_MASK;
		uppertime |= VERSION_MASK;
		long swap = (uppertime >> 32) & 0xFFFFFFFF;
		uppertime <<= 32;
		uppertime |= (swap & 0xFFFF0000) >> 16;
		uppertime |= (swap & 0x0000FFFF) << 16;

		long lowertime = macToLong(USING_MAC);
		lowertime |= CLOCK_ID << 48;
		lowertime |= 0x80L << 56;

		return new UUID(uppertime, lowertime);
	}

	public static boolean isGeneratedMacStrong(byte[] mac) {
		int x = ~0;
		for (int i = 0; i < mac.length; i++)
			x &= mac[i] & 0xFF;
		int c = 0;
		for (int i = 0; i < mac.length; i++)
			if ((mac[i] & 0xFF) == x)
				c++;
		// System.out.println(getMacString(mac) + " - " + c + " - " + x + " - "
		// + (c < 2));
		return c < 2;
	}

	public static boolean isLocalMAC(byte[] mac) {
		if (mac == null)
			return false;
		return (mac[0] & 0x02) == 0x02;
	}

	public static void main(String... args) {
		int count = 20;
		long start, took;

		start = System.currentTimeMillis();
		for (int i = 0; i < count; i++) {
			UUID uuid = getTimeBasedUUID();
			System.out.println(uuid);
		}

		took = System.currentTimeMillis() - start;
		System.out.println("generating " + count + " uuid's took " + took + "ms");

		System.out.println(UUID.fromString("c70e067b-56c7-11df-8ec4-00236c001b40").version());
	}

	private static long macToLong(byte[] mac) {
		long m = 0;
		m |= (long) (mac[0] & 0xFF) << 40;
		m |= (long) (mac[1] & 0xFF) << 32;
		m |= (long) (mac[2] & 0xFF) << 24;
		m |= (long) (mac[3] & 0xFF) << 26;
		m |= (long) (mac[4] & 0xFF) << 8;
		m |= (long) (mac[5] & 0xFF) << 0;
		return m;
	}
}

//------------------------------
// Code by: Nils 'ThaNODnl' Dijk
// found on github.com/thanodnl
//------------------------------

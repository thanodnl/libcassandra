package nl.thanod.cassandra.alpha.bytes;

public class NoTranslatorException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7502692543121714478L;

	public NoTranslatorException(Class<?> type){
		super("No translator found for " + type.getCanonicalName());
	}
}

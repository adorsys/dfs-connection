package de.adorsys.dfs.connection.api.domain;

public interface Payload {

	/**
	 * conveniance Method. Delivers the whole input stream, as long as its size
	 * is below the THRESH_HOLD
	 */
	byte[] getData();
}

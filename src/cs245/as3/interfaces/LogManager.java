package cs245.as3.interfaces;

public interface LogManager {
	// During testing, all methods of LogManager might throw
        // CrashException, which is a custom RuntimeException subclass we
        // are using for testing and simulating crashes. You are not expected to
	// catch any of these. Simply allow them to be caught by the test driver.

	/**
	 * @return the offset of the end of the log
	 */
	public int getLogEndOffset();

	/**
	 * Reads from log at the specified position.
	 * @return bytes in the log record in the range [offset, offset + size)
	 */
	public byte[] readLogRecord(int offset, int size);

	/**
	 * Atomically appends and persists record to the end of the log (implying that all previous appends have succeeded).
	 * @param record
	 * @return the log length prior to the append
	 */
	public int appendLogRecord(byte[] record);

	/**
	 * @return the current log truncation offset
	 */
	public int getLogTruncationOffset();

	/**
	 * Durably stores the offset as the current log truncation offset and truncates (deletes) the log up to that point.
	 * You can assume this occurs atomically. The test code will never call this.
	 */
	public void setLogTruncationOffset(int offset);
}

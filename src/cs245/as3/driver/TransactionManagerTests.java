package cs245.as3.driver;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Random;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import com.github.tkutche1.jgrade.gradedtest.GradedTest;

import cs245.as3.TransactionManager;
import cs245.as3.driver.LogManagerImpl.CrashException;

/**
 * DO NOT MODIFY THIS FILE IN THIS PACKAGE **
 */
public class TransactionManagerTests {
	
	//Test seeds will be modified by the autograder
    protected static long[] TEST_SEEDS = new long[] {0x12345671234567L, 0x1000, 42, 9};
    
	@Rule
    public Timeout globalTimeout = Timeout.seconds(60);
	
	public void TestTransactionTemplate(boolean check_recovery) {
    	int errors = 0;
    	
		LogManagerImpl lm = new LogManagerImpl();
		StorageManagerImpl sm = new StorageManagerImpl();
		//Simulate a storage manager that doesn't persist writes to a particular key
		//Comment this out and the test should succeed regardless of whether recover is implemented properly.
		sm.blockPersistenceForKeys(new long[] {10});
		byte[] testValue = new byte [] {1,2,3,4,5,6,7,8};
		{
			TransactionManager tm = new TransactionManager();
			sm.in_recovery = true;
			tm.initAndRecover(sm, lm);
			sm.in_recovery = false;
			
			long txID = 1;
			tm.start(txID);
			long key = 10;
			tm.write(txID, key, testValue);
			byte[] readA = tm.read(txID, key);
			System.out.println(Arrays.toString(readA)); //should return null
			if (readA != null) {
				System.out.println("Read uncommitted write.");
				errors++;
			}
			tm.commit(txID);
			byte[] readB = tm.read(txID, key);
			System.out.println(Arrays.toString(readB)); //should return 1 .. 8
			if (!Arrays.equals(testValue, readB)) {
				System.out.println("Did not read commited write.");
				errors++;
			}
		}
		if (check_recovery) {
			//Manually call persistence once:
			sm.do_persistence_work();
			sm.crash();
			{
				TransactionManager tm = new TransactionManager();
				sm.in_recovery = true;
				tm.initAndRecover(sm, lm);
				sm.in_recovery = false;
				long txID = 2;
				tm.start(txID);
				long key = 10;
				byte[] readC = tm.read(txID, key);
				System.out.println(Arrays.toString(readC)); //should return 1 .. 8
				if (!Arrays.equals(testValue, readC)) {
					System.out.println("Did not recover committed write.");
					errors++;
				}
			}
		}
		assert(errors == 0);
	}
	
	@Test
    @GradedTest(name="TestTransaction", number="1", points=0)
    public void TestTransaction() {
		TestTransactionTemplate(false);
	}
	
    @Test
    @GradedTest(name="TestRecovery", number="2", points=3.0)
    public void TestRecovery() {
    	TestTransactionTemplate(true);
    }
    
    public void TestTransaction2Template(boolean check_recovery) {
		LogManagerImpl lm = new LogManagerImpl();
		StorageManagerImpl sm = new StorageManagerImpl();
		sm.blockPersistenceForKeys(new long[] {990,991,992,993,994,995,996,997,998,999});
		TransactionManager tm = new TransactionManager();
		sm.in_recovery = true;
		tm.initAndRecover(sm, lm);
		sm.in_recovery = false;

		int nTXNs = 1000;    	
    	for(int i = 0; i < nTXNs; i+=2) {
    		long TXid1 = i;
    		long TXid2 = i + 1;
    		tm.start(TXid1);
    		tm.write(TXid1, i, String.format("%d padding",i).getBytes());
    		tm.start(TXid2);
    		{
    			//Read the value under txid2 that txid1 just wrote to:
    			byte[] readOther = tm.read(TXid2, i);
    			if (readOther != null) {
    				System.out.println("Could read uncommitted write.");
    			}
    			assert(readOther == null);
    		}
    		tm.commit(TXid1);
    		if (i % 100 >= 98) {
    			//Occasionally do persistence:
    			sm.do_persistence_work();
    		}
    		{
    			//Read the value under txid2 that txid1 just committed:
    			byte[] readOther = tm.read(TXid2, i);
        		assert(readOther != null);
        		String svalue = new String(readOther);
        		long lvalue = Long.parseLong(svalue.split(" ")[0]);
        		assert(lvalue == i);
    		}
    		tm.write(TXid2, i + 1, String.format("%d padding",i + 1).getBytes());
    		tm.commit(TXid2);
    	}
    	long TXidForRead = nTXNs + 1;
    	tm.start(TXidForRead);
    	for(int i = 0; i < nTXNs; i++) {
    		byte[] value = tm.read(TXidForRead, i);
    		assert(value != null);
    		String svalue = new String(value);
    		long lvalue = Long.parseLong(svalue.split(" ")[0]);
    		assert(lvalue == i);
    	}
    	if (check_recovery) {
	    	sm.crash();
	
			tm = new TransactionManager();
			sm.in_recovery = true;
			tm.initAndRecover(sm, lm);
			sm.in_recovery = false;
	    	TXidForRead = nTXNs + 2;
	    	tm.start(TXidForRead);
	    	for(int i = 0; i < nTXNs; i++) {
	    		byte[] value = tm.read(TXidForRead, i);
	    		assert(value != null);
	    		byte[] sm_value = sm.readLatestValue(i);
	    		assert(Arrays.equals(sm_value, value));
	    		String svalue = new String(value);
	    		long lvalue = Long.parseLong(svalue.split(" ")[0]);
	    		assert(lvalue == i);
	    	}
    	}
	}

	@Test
    @GradedTest(name="TestTransaction2", number="1", points=0)
    public void TestTransaction2() {
		TestTransaction2Template(false);
	}
	
    @Test
    @GradedTest(name="TestRecovery2", number="2", points=3.0)
    public void TestRecovery2() {
    	TestTransaction2Template(true);
    }
    
    public void TestBigWriteTemplate(final Random seeds, boolean check_recovery) {
		LogManagerImpl lm = new LogManagerImpl();
		StorageManagerImpl sm = new StorageManagerImpl();
		sm.blockPersistenceForKeys(new long[] {990,991,992,993,994,995,996,997,998,999});
		TransactionManager tm = new TransactionManager();
		sm.in_recovery = true;
		tm.initAndRecover(sm, lm);
		sm.in_recovery = false;

		Random r = new Random(seeds.nextLong());

		int nTXNs = 1000;    	
    	for(int i = 0; i < nTXNs; i+=2) {
    		long TXid1 = i;
    		long TXid2 = i + 1;
    		tm.start(TXid1);
    		byte[] value = new byte[100];
    		r.nextBytes(value);
    		
    		tm.write(TXid1, i, value);
    		tm.start(TXid2);
    		{
    			//Read the value under txid2 that txid1 just wrote to:
    			byte[] readOther = tm.read(TXid2, i);
    			if (readOther != null) {
    				System.out.println("Could read uncommitted write.");
    			}
    			assert(readOther == null);
    		}
    		tm.commit(TXid1);
    		if (i % 100 >= 98) {
    			//Occasionally do persistence:
    			sm.do_persistence_work();
    		}
    		{
    			//Read the value under txid2 that txid1 just committed:
    			byte[] readOther = tm.read(TXid2, i);
        		assert(readOther != null);
        		assert(Arrays.equals(readOther, value));
    		}
    		tm.write(TXid2, i + 1, value);
    		tm.commit(TXid2);
    	}
    	long TXidForRead = nTXNs + 1;
    	tm.start(TXidForRead);
    	for(int i = 0; i < nTXNs; i+=2) {
    		byte[] value = tm.read(TXidForRead, i);
    		assert(value != null);
    		byte[] ovalue = tm.read(TXidForRead, i+1);
    		assert(Arrays.equals(value, ovalue));
    	}
    	if (check_recovery) {
	    	sm.crash();
	
			tm = new TransactionManager();
			sm.in_recovery = true;
			tm.initAndRecover(sm, lm);
			sm.in_recovery = false;
	    	TXidForRead = nTXNs + 2;
	    	tm.start(TXidForRead);
	    	for(int i = 0; i < nTXNs; i+=2) {
	    		byte[] value = tm.read(TXidForRead, i);
	    		assert(value != null);
	    		byte[] ovalue = tm.read(TXidForRead, i+1);
	    		assert(Arrays.equals(value, ovalue));
	    	}
    	}
	}

	@Test
    @GradedTest(name="TestBigWrite", number="1", points=0)
    public void TestBigWrite() {
		Random seeds = new Random(TEST_SEEDS[0]);
		TestBigWriteTemplate(seeds, false);
		TestBigWriteTemplate(seeds, false);
		TestBigWriteTemplate(seeds, false);
	}
	
    @Test
    @GradedTest(name="TestBigWriteRecovery", number="2", points=3.0)
    public void TestBigRecovery() {
		Random seeds = new Random(TEST_SEEDS[1]);
    	TestBigWriteTemplate(seeds, true);
    	TestBigWriteTemplate(seeds, true);
    	TestBigWriteTemplate(seeds, true);
    }
    
    /**
      * Test that the transaction manager is doing something smart with log truncation
      */
    @Test
    @GradedTest(name="TestRecoveryPerformance", number="4", points=5.0)
    public void TestRecoveryPerformance() {
		LogManagerImpl lm = new LogManagerImpl();
		StorageManagerImpl sm = new StorageManagerImpl();
		TransactionManager tm = new TransactionManager();
		sm.setPersistenceListener(tm);
		sm.in_recovery = true;
		tm.initAndRecover(sm, lm);
		sm.in_recovery = false;
		
		Random r = new Random(TEST_SEEDS[0]);

		int key = r.nextInt(100);
		
		int maxLogSizeUntruncated = 0;
		
		int nTXNs = 990000 + r.nextInt(10000);
    	for(int i = 0; i < nTXNs + 1; i++) {
    		long TXid = i;
    		tm.start(TXid);
			tm.write(TXid, key, String.format("padding %d",i).getBytes());
			if (i == nTXNs) {
				//Have the last transaction crash before commit
				break;
			}
    		tm.commit(TXid);
    		//Stop persistence early to leave something to recover:
    		if (i % 100 >= 99 && i + 100 < nTXNs) {
    			//Safe point to update the checkpoint:
    			sm.do_persistence_work();
    		}
			//See how big the log is now:
			maxLogSizeUntruncated = Math.max(maxLogSizeUntruncated,  lm.getLogEndOffset() - lm.getLogTruncationOffset());
    	}
    	sm.crash();
    	
    	//Measure IOPs during recovery
    	tm = new TransactionManager();
    	int iopCount_now = lm.getIOPCount();
		sm.setPersistenceListener(tm);
		sm.in_recovery = true;
		tm.initAndRecover(sm, lm);
		sm.in_recovery = false;
    	int iopCount_delta = lm.getIOPCount() - iopCount_now;
		System.out.printf("Recovery used %d iops on the log.\n", iopCount_delta);
		System.out.printf("Maximum log size reached during workload: %d\n", maxLogSizeUntruncated);
		
		//Measure correctness of recovery:
		long TXidForRead = nTXNs + 10;
		byte[] value = tm.read(TXidForRead, key);
		assert(value != null);
		byte[] sm_value = sm.readLatestValue(key);
		assert(Arrays.equals(sm_value, value));
		String svalue = new String(value);
		long lvalue = Long.parseLong(svalue.split(" ")[1]);
		assert(lvalue == nTXNs - 1);
		
		//Test fails if recovery used too many log iops:
		assert(iopCount_delta < 1000);
		//... or if the log ever became too long:
		assert(maxLogSizeUntruncated < 50000);
	}
    
    /**
     * A slightly more complicated workflow with interleaved txns and big writes, where we also test recovery performance.
     */
   @Test
   @GradedTest(name="TestRecoveryPerformance2", number="4", points=5.0)
   public void TestRecoveryPerformance2() {
		LogManagerImpl lm = new LogManagerImpl();
		StorageManagerImpl sm = new StorageManagerImpl();
		TransactionManager tm = new TransactionManager();
		sm.setPersistenceListener(tm);
		sm.in_recovery = true;
		tm.initAndRecover(sm, lm);
		sm.in_recovery = false;
		
		Random r = new Random(TEST_SEEDS[1]);

		int maxLogSizeUntruncated = 0;
		
		int nTXNs = 99000 + r.nextInt(1000);
		long lastSuccessfulKey = 0;
		byte[] lastSuccessfulValue = null;
	   	for(int i = 0; i < nTXNs;) {
	   		assert(!Thread.interrupted()); //Cooperate with timeout:
	   		//Interleave some number of txns:
	   		int nToInterleave = r.nextInt(5) + 3;
	   		boolean shouldCrash = false;
	   		if (i + nToInterleave >= nTXNs) {
	   			//Have the last set crash.
	   			shouldCrash = true;
	   			lm.stopServingRequestsAfterIOs(2);
	   		}
	   		try {
		   		for(int j = 0; j < nToInterleave; j++) {
		   			long TXid = i + j;
		   			tm.start(TXid);
		   			long key = r.nextInt(500);
		   			tm.read(TXid, key);
		   		}
		   		for(int j = 0; j < nToInterleave; j++) {
		   			long TXid = i + j;
		   			long key = r.nextInt(500);
		   			byte[] value = new byte[100];
		   			r.nextBytes(value);
		   			if (j == nToInterleave - 1 && !shouldCrash) {
		   				//Then j's write should make it in.
		   				lastSuccessfulKey = key;
		   				lastSuccessfulValue = value;
		   			}
		   			tm.write(TXid, key, value);
		   		}
		   		for(int j = 0; j < nToInterleave; j++) {
		   			long TXid = i + j;
		   			boolean shouldAbort = r.nextBoolean() && (j != nToInterleave - 1);
		   			if (shouldAbort) {
		   				tm.abort(TXid);
		   			} else {
		   				tm.commit(TXid);
		   			}
		   		}
	   		} catch (CrashException e) {
	   			//Ignore expected crash exception.
	   		}
	   		//Stop persistence early to leave something to recover:
	   		if (i % 100 >= (100 - nToInterleave) && i + 100 < nTXNs) {
	   			//Safe point to update the checkpoint:
	   			sm.do_persistence_work();
	   		}
			//See how big the log is now:
			maxLogSizeUntruncated = Math.max(maxLogSizeUntruncated,  lm.getLogEndOffset() - lm.getLogTruncationOffset());
			
			//Advance
		   	i += nToInterleave;
	   	}
	   	
	   	sm.crash();
	   	lm.resumeServingRequests();
   	
	   	//Measure IOPs during recovery
	   	tm = new TransactionManager();
	   	int iopCount_now = lm.getIOPCount();
		sm.setPersistenceListener(tm);
		sm.in_recovery = true;
		tm.initAndRecover(sm, lm);
		sm.in_recovery = false;
		int iopCount_delta = lm.getIOPCount() - iopCount_now;
		System.out.printf("Recovery used %d iops on the log.\n", iopCount_delta);
		System.out.printf("Maximum log size reached during workload: %d\n", maxLogSizeUntruncated);
		
		//Measure correctness of recovery:
		long TXidForRead = nTXNs + 10;
		byte[] value = tm.read(TXidForRead, lastSuccessfulKey);
		assert(value != null);
		byte[] sm_value = sm.readLatestValue(lastSuccessfulKey);
		assert(Arrays.equals(sm_value, value));
		assert(Arrays.equals(value,  lastSuccessfulValue));
		
		//Test fails if recovery used too many log iops:
		assert(iopCount_delta < 500);
		//... or if the log ever became too long:
		assert(maxLogSizeUntruncated < 30000);
	}
   
    public void TestRepeatedFailuresTemplate(final Random seeds, double failureRate) {
		LogManagerImpl lm = new LogManagerImpl();
		StorageManagerImpl sm = new StorageManagerImpl();
		TransactionManager tm = new TransactionManager();
		sm.setPersistenceListener(tm);
		sm.in_recovery = true;
		tm.initAndRecover(sm, lm);
		sm.in_recovery = false;
		
		Random r = new Random(seeds.nextLong());

		//Track what the correct values are for each key across crashes for testing purposes:
		HashMap<Long, byte[]> lastCommittedValues = new HashMap();
		
		//Really stress recovery after repeated crashes. We make sure to succeed at least the first time.
		//By selecting these keys to be persisted, we ensure we always have to replay the whole log,
		//while also testing commits that don't handle some keys but not others being persisted.
		sm.blockPersistenceForKeys(new long[] {0,2,4,6,8,10,12,14});
		int nTrials = 50;
		int numCrashes = 0;
		for(int trial = 0; trial < nTrials; trial++) {
			int numWrites = 10 + r.nextInt(5);
			long writeOffset = r.nextInt(5);
			long TXid = trial * 2;
			tm.start(TXid);
			boolean shouldCrash = (r.nextDouble() < failureRate) && (trial != 0);
			boolean shouldAbort = r.nextBoolean() && (trial != 0); //Just to see if they're doing something weird with abort.
			if (shouldCrash) {
				lm.stopServingRequestsAfterIOs(5);
			}
			try {
				for(int i = 0; i < numWrites; i++) {
					byte[] value = new byte[100];
					r.nextBytes(value);
					if (!shouldCrash && !shouldAbort) {
						lastCommittedValues.put(i + writeOffset, value);
					}
					tm.write(TXid, i + writeOffset, value);
				}
				if (shouldAbort) {
					tm.abort(TXid);
				} else {
					tm.commit(TXid);
				}
			} catch (CrashException e) {
				//Eat the expected CrashException thrown by the crash.
			}
			if (shouldCrash) {
				//Sometimes make persistence happen, sometimes don't!
				if (r.nextBoolean()) {
					sm.do_persistence_work();
				}
				sm.crash();
				//Resume processing:
				lm.resumeServingRequests();
				sm.blockPersistenceForKeys(null);
				tm = new TransactionManager();
				sm.setPersistenceListener(tm);
				sm.in_recovery = true;
				tm.initAndRecover(sm, lm);
				sm.in_recovery = true;
				numCrashes++;
			}
			//Check that the TransactionManager and StorageManager both match 
			//the values in lastCommittedValues
			long TXidforRead = trial * 2 + 1;
			tm.start(TXidforRead);
			for(Entry<Long, byte[]> entry : lastCommittedValues.entrySet()) {
				String crashesDescription="V";
				if (numCrashes > 0) {
					crashesDescription=String.format("After %d crashes, v", numCrashes);
				}
				byte[] read = tm.read(TXidforRead, entry.getKey());
				if (!Arrays.equals(entry.getValue(), read)) {
					System.out.printf("%salue for key %d did not match what was committed (wanted %s..., got %s...).",
							crashesDescription,
							entry.getKey(),
							Arrays.toString(entry.getValue()).substring(0, 10),
							read == null ? "null" : Arrays.toString(read).substring(0, 10)
					);
					assert(false);
				}
			}
			tm.abort(TXidforRead);
		}
	}
    
    /**
      * Test that the transaction manager is doing something smart with log truncation
      */
    @Test
    @GradedTest(name="TestRepeatedFailures", number="4", points=3.0)
    public void TestRepeatedFailures() {
    	Random seeds = new Random(TEST_SEEDS[2]);
    	for(int i = 0; i < 10; i++) {
    		TestRepeatedFailuresTemplate(seeds, 0.1);
    	}
    }
    
    /**
      * Test with even more failures.
      */
    @Test
    @GradedTest(name="TestRepeatedFailures2", number="4", points=3.0)
    public void TestRepeatedFailures2() {
    	Random seeds = new Random(TEST_SEEDS[3]);
    	for(int i = 0; i < 10; i++) {
    		TestRepeatedFailuresTemplate(seeds, 0.5);
    	}
    }
}

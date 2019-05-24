package cs245.as3.driver;

import java.util.Random;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import com.github.tkutche1.jgrade.gradedtest.GradedTest;

import cs245.as3.TransactionManager;
import cs245.as3.driver.LogManagerImpl.CrashException;
import cs245.as3.driver.Workloads.Transaction;

public class InterleavedTransactionManagerTests {
	
	//Test seeds will be modified by the autograder
    protected static long[] TEST_SEEDS = new long[] {0x12345671234567L, 0x456789abcdefL, 0x57194823L, 0xf00b44};
	
    @Rule
    public Timeout globalTimeout = Timeout.seconds(60);
	
    /**
      * Runs transactions on multiple threads and ensures that writes are committed transactionally
      */
    public void TestTransactionsTemplate(final Random seeds, boolean check_recovery, @SuppressWarnings("rawtypes") Class transactionType) {
		final LogManagerImpl lm = new LogManagerImpl();
		final StorageManagerImpl sm = new StorageManagerImpl();
		
		int nSteps = 100000;
		Random r = new Random(seeds.nextLong());
		//Make T1 slightly slower or faster than T2. This is the cutoff on whether a step is done on T1 or T2.
		double T1bias = r.nextDouble()/2 + 0.05; //[0.05,0.55)
		int failLogAfterSteps = nSteps - r.nextInt(100) - 10;
		class Workload {
			public int thrID;
			public Random r;
			public Workloads.Transaction lastTxn;
			public Workloads.Transaction lastTxnCommitted;
			public boolean success;
			public int TXid;

			//the startTXid must be spaced far enough apart to ensure TXids never overlap.
			@SuppressWarnings("unchecked")
			public Workload(int thrID, int startTXid) {
				this.thrID = thrID;
				r = new Random(seeds.nextLong());
				lastTxn = null;
				lastTxnCommitted = null;
				success = false;
				TXid = startTXid;
			}
			
			public void step(TransactionManager tm) {
				if (lastTxn == null) {
					try {
						lastTxn = (Transaction) transactionType.getConstructor().newInstance();
					} catch (Throwable e) {
						throw new RuntimeException(e.getMessage());
					}
					lastTxn.generate(r);
				}
				if (lastTxn.on_commit_step()) {
					//Back up the parameters:
					if (lastTxnCommitted == null) {
						try {
							lastTxnCommitted = (Transaction) transactionType.getConstructor().newInstance();
						} catch (Throwable e) {
							throw new RuntimeException(e.getMessage());
						}
					}
					lastTxnCommitted.copy(lastTxn);
					//This will do the commit:
					lastTxn.step(TXid, tm);
					//Start a new transaction:
					lastTxn.generate(r);
					TXid++;
				} else {
					lastTxn.step(TXid, tm);
				}
			}
		}
		
		Workload T1 = new Workload(0,0);
		Workload T2 = new Workload(1,nSteps);
		//Assigned below
		Transaction T1LastTxnCommitted;
		Transaction T2LastTxnCommitted;
		{
			TransactionManager tm = new TransactionManager();
			sm.setPersistenceListener(tm);
			long[] blockedKeys = new long[20];
			{
				for(int i = 0; i < blockedKeys.length; i++) {
					blockedKeys[i] = r.nextInt(100);
				}
			}
			sm.blockPersistenceForKeys(blockedKeys);
			sm.in_recovery = true;
			tm.initAndRecover(sm, lm);
			sm.in_recovery = false;
			
			int i = 0;
			for(; i < nSteps; i++){
				if (r.nextDouble() < T1bias) {
					T1.step(tm);
				} else {
					T2.step(tm);
				}
				if (i > failLogAfterSteps) {
					break;
				}
				if (i % 100 >= 99) {
					//Do persistence work
					sm.do_persistence_work();
				}
			}
			int TXidForRead = nSteps * 2 + 1;
			assert(T1.lastTxnCommitted != null);
			assert(T2.lastTxnCommitted != null);
			tm.start(TXidForRead);
			if (T1.lastTxnCommitted.check(TXidForRead, tm)) {
				System.out.println("T1's last TXN is committed.");
			} else if (T2.lastTxnCommitted.check(TXidForRead, tm)){
				System.out.println("T2's last TXN is committed.");
			} else {
				System.out.println("Couldn't read the last commit of either thread.");
				assert(false);
			}
			//Sanity check.
			assert(i > failLogAfterSteps);
			//We will definitely not commit any future transactions.
			lm.stopServingRequestsAfterIOs(1);
			T1LastTxnCommitted = T1.lastTxnCommitted.clone();
			T2LastTxnCommitted = T2.lastTxnCommitted.clone();
			try {
				for(; i < nSteps; i++) {
					//We don't call do_persistence_work here so that we have some unpersisted queued writes
					if (r.nextDouble() < T1bias) {
						T1.step(tm);
					} else {
						T2.step(tm);
					}
				}
			} catch (CrashException e) {
				//We expect to fail here.
			}
			if (check_recovery) {
				//Check if either txn has been queued to the storage manager (one of them should be assuming a 
				//durable implementation.)
				if (T1LastTxnCommitted.checkWritesQueued(TXidForRead, sm)) {
					System.out.println("T1's last committed TXN is queued to the storage manager.");
				} else if (T2LastTxnCommitted.checkWritesQueued(TXidForRead, sm)){
					System.out.println("T2's last committed TXN is queued to the storage manager.");
				} else {
					System.out.println("Neither threads' last commit was queued to the storage manager - we're going to call that an error.");
					assert(false);
				}
			}
		}
			
    	if (check_recovery){
    		sm.do_persistence_work();
    		sm.crash();
    		lm.resumeServingRequests();
    		
        	TransactionManager tm = new TransactionManager();
			sm.in_recovery = true;
			tm.initAndRecover(sm, lm);
			sm.in_recovery = false;
			int TXidForRead = nSteps * 2 + 2;
	    	tm.start(TXidForRead);
			if (T1LastTxnCommitted.check(TXidForRead, tm)) {
				System.out.println("T1's last committed TXN is visible after recovery.");
			} else if (T2LastTxnCommitted.check(TXidForRead, tm)){
				System.out.println("T2's last committed TXN is visible after recovery.");
			} else {
				System.out.println("Couldn't read the last commit of either thread after recovery.");
				assert(false);
			}
			//We also check that the storage manager is being used properly
			if (T1LastTxnCommitted.checkWritesQueued(TXidForRead, sm)) {
				System.out.println("T1's last committed TXN is queued to the storage manager after recovery.");
			} else if (T2LastTxnCommitted.checkWritesQueued(TXidForRead, sm)){
				System.out.println("T2's last committed TXN is queued to the storage manager after recovery.");
			} else {
				System.out.println("Neither threads' last commit was queued to the storage manager after recovery - we're going to call that an error.");
				assert(false);
			}
    	}
	}

    @Test
    @GradedTest(name="TestCoupledWrites", number="2", points=0.0)
    public void TestCoupledWrites(){
    	Random seeds = new Random(TEST_SEEDS[0]);
    	TestTransactionsTemplate(seeds, false, Workloads.CoupledWritesTransaction.class);
    	TestTransactionsTemplate(seeds, false, Workloads.CoupledWritesTransaction.class);
    	TestTransactionsTemplate(seeds, false, Workloads.CoupledWritesTransaction.class);
    }

    @Test
    @GradedTest(name="TestCoupledWritesRecovery", number="2", points=3.0)
    public void TestCoupledWritesRecovery(){
    	Random seeds = new Random(TEST_SEEDS[1]);
    	TestTransactionsTemplate(seeds, true, Workloads.CoupledWritesTransaction.class);
    	TestTransactionsTemplate(seeds, true, Workloads.CoupledWritesTransaction.class);
    	TestTransactionsTemplate(seeds, true, Workloads.CoupledWritesTransaction.class);
    }

    @Test
    @GradedTest(name="TestBigTransactions", number="2", points=3.0)
    public void TestBigTransactions(){
    	Random seeds = new Random(TEST_SEEDS[2]);
    	TestTransactionsTemplate(seeds, false, Workloads.BigTransaction.class);
    	TestTransactionsTemplate(seeds, false, Workloads.BigTransaction.class);
    	TestTransactionsTemplate(seeds, false, Workloads.BigTransaction.class);
    }

    @Test
    @GradedTest(name="TestBigTransactionsRecovery", number="2", points=3.0)
    public void TestBigTransactionsRecovery(){
    	Random seeds = new Random(TEST_SEEDS[3]);
    	TestTransactionsTemplate(seeds, true, Workloads.BigTransaction.class);
    	TestTransactionsTemplate(seeds, true, Workloads.BigTransaction.class);
    	TestTransactionsTemplate(seeds, true, Workloads.BigTransaction.class);
    }
}

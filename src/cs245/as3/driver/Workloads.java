package cs245.as3.driver;

import java.util.Arrays;
import java.util.Random;

import cs245.as3.TransactionManager;

public class Workloads {
	
	public static interface Transaction {
		public void generate(Random r);
		public boolean on_commit_step();
		public void step(long TXid, TransactionManager tm);
		public boolean check(long TXid, TransactionManager tm);
		public boolean checkWritesQueued(long TXid, StorageManagerImpl sm);
		public void copy(Transaction lastTxn);
		public Transaction clone();
	}

	/**
	  * write(x, a)
	  * write(y, b)
	  * If (order) do the writes in the opposite order
	  */
	public static class CoupledWritesTransaction implements Transaction {
		public long x;
		public long y;
		public byte[] a;
		public byte[] b;
		public boolean order;
		private int step = 0;

		public void generate(Random r) {
			x = r.nextInt(100);
			y = r.nextInt(100);
			a = String.format("%d padding", r.nextInt(100)).getBytes();
			b = String.format("%d padding", r.nextInt(100)).getBytes();
			order = r.nextBoolean();
		}
		
		public void step(long txID, TransactionManager tm) {
			if (step == 0) tm.start(txID); 
			if (!order) {
				if (step == 1) tm.write(txID, x, a);
				if (step == 2) tm.write(txID, y, b);
			} else {
				if (step == 1) tm.write(txID, y, b);
				if (step == 2) tm.write(txID, x, a);
			}
			if (step == 3) { 
				tm.commit(txID); step = 0;
			} else {
				step++;
			}
		}

		public boolean on_commit_step() {
			return step == 3;
		}

		public void copy(Transaction _other) {
			CoupledWritesTransaction o = (CoupledWritesTransaction)_other;
			x=o.x;
			y=o.y;
			a=o.a;
			b=o.b;
			order=o.order;
		}

		public Transaction clone() {
			Transaction toRet = new CoupledWritesTransaction();
			toRet.copy(this);
			return toRet;
		}

		public boolean check(long txID, TransactionManager tm) {
			return Arrays.equals(tm.read(txID, x), a) && 
					Arrays.equals(tm.read(txID, y), b)
			;
		}

		public boolean checkWritesQueued(long txID, StorageManagerImpl sm) {
			return Arrays.equals(sm.readLatestValue(x), a) && 
					Arrays.equals(sm.readLatestValue(y), b)
			;
		}
	}
	
	/**
	  * write(x + i, a) for i in 1..10
	  * If (order) do the writes in reverse order
	  */
	public static class BigTransaction implements Transaction {
		public long x;
		public byte[] a;
		public boolean order;
		private int step = 0;

		public void generate(Random r) {
			x = r.nextInt(100);
			a = String.format("padding %d", r.nextInt(100)).getBytes();
			order = r.nextBoolean();
		}
		
		public void step(long txID, TransactionManager tm) {
			if (step == 0) tm.start(txID);
			if (step > 0 && !on_commit_step()) {
				if (!order) {
					long key = x + step - 1; //ith write is when step == i
					tm.write(txID, key, a);
				} else {
					long key = x + 10 - step; //ith write is when step == i
					tm.write(txID, key, a);
				}
			}
			if (on_commit_step()) {
				//Just to break things, re-write to the first word:
				tm.write(txID, x, a);
				tm.commit(txID);
				step = 0;
			} else {
				step++;
			}
		}
		
		public boolean on_commit_step() {
			return step == 11; //commit 10 writes, ith write is when step == i
		}

		public void copy(Transaction _other) {
			BigTransaction o = (BigTransaction)_other;
			x = o.x;
			a = o.a;
			order = o.order;
		}
		
		public Transaction clone() {
			Transaction toRet = new BigTransaction();
			toRet.copy(this);
			return toRet;
		}
		
		@Override
		public boolean check(long TXid, TransactionManager tm) {
			for(long key = x; key < x + 10; key++) {
				if (!Arrays.equals(tm.read(TXid, key), a)) return false;
			}
			return true;
		}

		@Override
		public boolean checkWritesQueued(long TXid, StorageManagerImpl sm) {
			for(long key = x; key < x + 10; key++) {
				if (!Arrays.equals(sm.readLatestValue(key), a)) return false;
			}
			return true;
		}
	}
}

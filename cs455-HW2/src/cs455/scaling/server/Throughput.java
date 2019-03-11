package cs455.scaling.server;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Helper class for managing the statistics to be tracked for the throughput of messages
 */

public class Throughput {

	private AtomicInteger messageThroughputAtomic = new AtomicInteger(0);
	private static final double MESSAGE_RATE = 20.0;
	
	public Throughput() { }
	
	public void incrementMessageThroughput() {
		this.messageThroughputAtomic.incrementAndGet();
	}
	
	public synchronized double getMessageThroughput() {
		double returnThroughput = this.messageThroughputAtomic.get() / MESSAGE_RATE;
		resetThroughput();
		return returnThroughput;
	}
	
	private void resetThroughput() {
		this.messageThroughputAtomic.set(0);
	}
 }

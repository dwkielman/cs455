package cs455.scaling.server;

public class Throughput {

	private int messageThroughput;
	private final double messageRate = 20.0;
	
	public Throughput() {
		this.messageThroughput = 0;
	}
	
	public synchronized void incrementMessageThroughput() {
		this.messageThroughput++;
	}
	
	public synchronized double getMessageThroughput() {
		double returnThroughput = this.messageThroughput / messageRate;
		resetThroughput();
		return returnThroughput;
	}
	
	private synchronized void resetThroughput() {
		this.messageThroughput = 0;
	}
}

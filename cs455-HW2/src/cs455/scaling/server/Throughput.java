package cs455.scaling.server;

public class Throughput {

	private int messageThroughput;
	private static final double MESSAGE_RATE = 20.0;
	private String status = "";
	
	public Throughput() {
		this.messageThroughput = 0;
	}
	
	public synchronized void incrementMessageThroughput() {
		this.messageThroughput++;
	}
	
	public synchronized double getMessageThroughput() {
		double returnThroughput = this.messageThroughput / MESSAGE_RATE;
		resetThroughput();
		return returnThroughput;
	}
	
	private synchronized void resetThroughput() {
		this.messageThroughput = 0;
	}
	
	public synchronized String getThroughputStatus() {
		return this.status;
	}
	
	public synchronized void setThroughputStatus(String status) {
		this.status = status;
	}
 }

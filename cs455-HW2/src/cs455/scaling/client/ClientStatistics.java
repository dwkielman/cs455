package cs455.scaling.client;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;

public class ClientStatistics implements Runnable {

	private LinkedList<String> hashCodes;
	private int messagesSent;
	private int messagesReceived;
	private final long messageRate = 20;
	private final DateTimeFormatter dateTimeFormat = DateTimeFormatter.ofPattern("MM/dd/yy HH:mm:ss");
	private final Object lock = new Object();
	
	public ClientStatistics() {
		this.messagesSent = 0;
		this.messagesReceived = 0;
		this.hashCodes = new LinkedList<String>();
	}
	
	public synchronized void incrementMessagesSent() {
		this.messagesSent++;
	}
	
	public synchronized void incrementMessagesReceived() {
		this.messagesReceived++;
	}
	
	public synchronized void resetClientStatistics() {
		this.messagesSent = 0;
		this.messagesReceived = 0;
	}
	
	public void addHashCode(String hashCode) {
		synchronized(hashCodes) {
			hashCodes.add(hashCode);
		}
	}
	
	public boolean removeHashCode(String hashCode) {
		synchronized(hashCodes) {
			if (this.hashCodes.contains(hashCode)) {
				this.hashCodes.remove(hashCode);
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Once every 20 seconds after starting up, every client should print the number of messages it
	 * has sent and received during the last 20 seconds. This log message should look similar to the following.
	 * [timestamp] Total Sent Count: x, Total Received Count: y
	 */
	@Override
	public void run() {
		// Create the time we will wait until
		LocalDateTime messageTime = LocalDateTime.now().plusSeconds(messageRate);
		while(true) {
			// get the current time
			LocalDateTime now = LocalDateTime.now();
			
			// only print when it's been 20 seconds since the last message
			if (now.isAfter(messageTime)) {
				messageTime = now.plusSeconds(messageRate);
				
				synchronized(lock) {
					// [timestamp] Total Sent Count: x, Total Received Count: y
					String currentClientMessage = "[" + messageTime.format(dateTimeFormat) + "]";
					currentClientMessage += "Total Sent Count: " + this.messagesSent + ", Total Received Count: " + this.messagesReceived;
					System.out.println(currentClientMessage);
					resetClientStatistics();
				}
			}
		}
	}
}

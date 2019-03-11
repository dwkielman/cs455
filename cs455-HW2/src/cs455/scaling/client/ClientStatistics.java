package cs455.scaling.client;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientStatistics implements Runnable {

	private LinkedList<String> hashCodes;
	private static final long MESSAGE_RATE = 20;
	private AtomicInteger messagesSentAtomic = new AtomicInteger(0);
	private AtomicInteger messagesReceivedAtomic = new AtomicInteger(0);
	private final DateTimeFormatter dateTimeFormat = DateTimeFormatter.ofPattern("MM/dd/yy HH:mm:ss");
	private final Object lock = new Object();
	
	public ClientStatistics() {
		this.hashCodes = new LinkedList<String>();
	}
	
	public void incrementMessagesSent() {
		this.messagesSentAtomic.incrementAndGet();
	}
	
	public void incrementMessagesReceived() {
		this.messagesReceivedAtomic.incrementAndGet();
	}
	
	public synchronized void resetClientStatistics() {
		this.messagesSentAtomic.set(0);
		this.messagesReceivedAtomic.set(0);
	}
	
	public void addHashCode(String hashCode) {
		synchronized(hashCodes) {
			hashCodes.add(hashCode);
		}
	}
	
	public boolean containsHashCode(String hashCode) {
		synchronized(hashCodes) {
			if (this.hashCodes.contains(hashCode)) {
				return true;
			} else {
				return false;
			}
		}
	}
	
	public void removeHashCode(String hashCode) {
		synchronized(hashCodes) {
			if (this.hashCodes.contains(hashCode)) {
				this.hashCodes.remove(hashCode);
			}
		}
	}
	
	/**
	 * Once every 20 seconds after starting up, every client should print the number of messages it
	 * has sent and received during the last 20 seconds. This log message should look similar to the following.
	 * [timestamp] Total Sent Count: x, Total Received Count: y
	 */
	@Override
	public void run() {
		// create the time we will wait until
		LocalDateTime messageTime = LocalDateTime.now().plusSeconds(MESSAGE_RATE);
		while(true) {
			// get the current time
			LocalDateTime now = LocalDateTime.now();
			
			// only print when it's been 20 seconds since the last message was printed
			if (now.isAfter(messageTime)) {
				messageTime = now.plusSeconds(MESSAGE_RATE);
				
				synchronized(lock) {
					String currentClientMessage = "[" + messageTime.format(dateTimeFormat) + "]";
					currentClientMessage += "Total Sent Count: " + this.messagesSentAtomic.get() + ", Total Received Count: " + this.messagesReceivedAtomic.get();
					System.out.println(currentClientMessage);
					resetClientStatistics();
				}
			}
		}
	}
}

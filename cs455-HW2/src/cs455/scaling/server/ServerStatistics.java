package cs455.scaling.server;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Every 20 seconds, the server should print its current throughput (number of messages processed per second during last 20 seconds), the number of active client connections, and mean and standard 
 * deviation of per-client throughput to the console. In order to calculate the per-client throughput statistics (mean and standard deviation), you need to maintain the throughputs for individual clients for last 20 
 * seconds (number of messages processed per second sent by a particular client during last 20 seconds) and calculate the mean and the standard deviation of those throughput values.
 * 
 * FORMAT:
 * [timestamp] Server Throughput: x messages/s, Active Client Connections: y, Mean Per client Throughput: p messages/s, Std. Dev. Of Per-client Throughput: q messages/s
 */

public class ServerStatistics implements Runnable {
	
	// process messages every 20 seconds
	private static final long MESSAGE_RATE = 20;
	private ArrayList<Throughput> activeClientsThroughputList;
	private AtomicInteger activeClientsAtomic = new AtomicInteger(0);
	private AtomicInteger serverThroughputAtomic = new AtomicInteger(0);
	private final Object lock = new Object();
	private final DateTimeFormatter dateTimeFormat = DateTimeFormatter.ofPattern("MM/dd/yy HH:mm:ss");;
	
	public ServerStatistics() {
		this.activeClientsThroughputList = new ArrayList<Throughput>();
	}
	
	public Throughput addClient() {
		synchronized (activeClientsThroughputList) {
			this.activeClientsAtomic.incrementAndGet();
			Throughput throughput = new Throughput();
			this.activeClientsThroughputList.add(throughput);
			return throughput;
		}
	}

	public void incremementServerThroughput() {
		this.serverThroughputAtomic.incrementAndGet();
	}
	
	private void resetServerThroughput() {
		this.serverThroughputAtomic.set(0);
	}
	
	private double calculateSD(ArrayList<Double> throughputList, double sum, double mean) {
        double standardDeviation = 0.0;

        for(double num: throughputList) {
            standardDeviation += Math.pow(num - mean, 2);
        }

        // avoiding divide by 0
        if (throughputList.size() > 0) {
        	return Math.sqrt(standardDeviation/(throughputList.size()));
        } else {
        	return 0.0;
        }
        
    }
	
	@Override
	public void run() {
		// Create the time we will wait until
		LocalDateTime messageTime = LocalDateTime.now().plusSeconds(MESSAGE_RATE);
			while (true) {
				// get the current time
				LocalDateTime now = LocalDateTime.now();
				// only print when it's been 20 seconds since the last message
				if (now.isAfter(messageTime)) {
					messageTime = now.plusSeconds(MESSAGE_RATE);
					
					synchronized (lock) {
						if (!this.activeClientsThroughputList.isEmpty()) {
							double runnableServerThroughput = this.serverThroughputAtomic.get() / MESSAGE_RATE;
							
							double totalClientThroughputSum = 0.0;
							double meanPerClientThroughput = 0.0;
							double sdPerClientThroughput = 0.0;
							
							ArrayList<Double> throughputList = new ArrayList<Double>();
							
							// calculate sum and create a list of current throughput per client
							for (Throughput throughput : this.activeClientsThroughputList) {
								double mtp = throughput.getMessageThroughput();
								totalClientThroughputSum += mtp;
								throughputList.add(mtp);
							}
							
							// calculate mean
							meanPerClientThroughput = totalClientThroughputSum / this.activeClientsThroughputList.size();
							
							// calculate standard deviation
							sdPerClientThroughput = calculateSD(throughputList, totalClientThroughputSum, meanPerClientThroughput);

							String currentThroughputMessage = "[" + messageTime.format(dateTimeFormat) + "]";
							currentThroughputMessage += " Server Throughput: " + runnableServerThroughput + " messages/s, Active Client Connections: " + this.activeClientsAtomic.get()  + ", Mean Per-client" + 
									" Throughput: " + meanPerClientThroughput + " messages/s, Std. Dev. Of Per-client Throughput: " + sdPerClientThroughput + " messages/s";
							
							System.out.println(currentThroughputMessage);
							resetServerThroughput();
					}
				}
			}
		}
	}
}

package cs455.scaling.server;

/**
 * There is exactly one server node in the system. The server node provides the following functions:
 * A. Accepts incoming network connections from the clients.
 * B. Accepts incoming traffic from these connections
 * C. Groups data from the clients together into batches
 * D. Replies to clients by sending back a hash code for each message received.
 * E. The server performs functions A, B, C, and D by relying on the thread pool.
 * 
 * Executed with the following command:
 * java cs455.scaling.server.Server portnum thread-pool-size batch-size batch-time
 *
 *
 */

public class Server {
	
	// Upon receiving the data, the server will compute the hash code for the data packet and send this back to the client
	
	// sends an acknoledgement to the client
	
	
	
	
	
	/**
	 * Every 20 seconds, the server should print its current throughput (number of messages processed per second during last 20 seconds), the number of active client connections, and mean and standard 
	 * deviation of per-client throughput to the console. In order to calculate the per-client throughput statistics (mean and standard deviation), you need to maintain the throughputs for individual clients for last 20 
	 * seconds (number of messages processed per second sent by a particular client during last 20 seconds) and calculate the mean and the standard deviation of those throughput values.
	 * 
	 * FORMAT:
	 * [timestamp] Server Throughput: x messages/s, Active Client Connections: y, Mean Per client Throughput: p messages/s, Std. Dev. Of Per-client Throughput: q messages/s
	 */

}

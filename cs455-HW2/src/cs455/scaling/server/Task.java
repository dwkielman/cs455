package cs455.scaling.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import cs455.scaling.hash.Hash;

/**
 * Server processes messages from Clients in the form of Tasks that are read by the Server, processed, and sent back to the Clients.
 */

public class Task {

	private static final int BUFFER_SIZE = 8192;
	private final SelectionKey key;
	private final ServerStatistics serverStatistics;
	private final Hash hash;
	private final Throughput throughput;
	private final ThreadPoolManager threadPoolManager;
	private String message;
	
	public Task(SelectionKey key, ServerStatistics serverStatistics, Hash hash, ThreadPoolManager threadPoolManager) {
		this.key = key;
		this.serverStatistics = serverStatistics;
		this.hash = hash;
		this.throughput = (Throughput) key.attachment();
		this.threadPoolManager = threadPoolManager;
	}
	
	private void setMessage(String message) {
		this.message = message;
	}
	
	private String getMessage() {
		return this.message;
	}
	
	public void readTask() {
		// Create a buffer to read into
        ByteBuffer readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
		int bytesRead = 0;
		readBuffer.clear();

        // Grab the socket from the key 
        SocketChannel channel = (SocketChannel) key.channel();
        
        // Read from it
        try {
        	// Read the whole thing
        	while (readBuffer.hasRemaining() && bytesRead != -1) {
        		bytesRead = channel.read(readBuffer);
        	}
        	
        	readBuffer.rewind();
        	
			// Handle a closed connection
	        if (bytesRead == -1) {
	            channel.close();
	            System.out.println("\t\tClient disconnected.");
	            return;
	        } else {
	            String hashedString = hash.SHA1FromBytes(readBuffer.array());
	            //System.out.println("Got a message from Client that reads: " + hashedString);
	            setMessage(hashedString);

	            // Clear the buffer
	            readBuffer.clear();
	        }
	    } catch (IOException ioe) {
			ioe.printStackTrace();
		}
        assignTaskToThreadPoolManager();
	}
	
	private void assignTaskToThreadPoolManager() {
		this.threadPoolManager.addTask(this, key);
	}
	
	// Upon receiving the data, the server will compute the hash code for the data packet and send this back to the client
	public void sendHashResponse() {
		// pad the message with 0s in case it needs it
		String paddedResponse = String.format("%40s", getMessage());
		ByteBuffer sendBuffer = ByteBuffer.wrap(paddedResponse.getBytes());
		
		 // Grab the socket from the key 
        SocketChannel channel = (SocketChannel) key.channel();
        
		try {
			sendBuffer.rewind();
			
			while (sendBuffer.hasRemaining()) {
				channel.write(sendBuffer);
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}

		this.throughput.incrementMessageThroughput();
        this.serverStatistics.incremementServerThroughput();
        
        sendBuffer.clear();
		
	}
}
package cs455.scaling.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import cs455.scaling.hash.Hash;

public class Task implements Runnable {

	private final int bufferSize = 8192;
	private final SelectionKey key;
	private final ServerStatistics serverStatistics;
	private final SocketChannel socketChannel;
	private final Hash hash;
	private final Throughput throughput;
	
	public Task(SelectionKey key, ServerStatistics serverStatistics, Hash hash) {
		this.key = key;
		this.serverStatistics = serverStatistics;
		this.hash = hash;
		this.socketChannel = (SocketChannel) key.channel();
		this.throughput = (Throughput) key.attachment();
	}

	@Override
	public void run() {
		// Create a buffer to read into
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
		int bytesRead = 0;
		buffer.clear();

        // Grab the socket from the key 
        SocketChannel client = (SocketChannel) key.channel();
        
        // Read from it
        try {
        	// read the whole thing
        	while (buffer.hasRemaining() && bytesRead != -1) {
        		bytesRead = client.read(buffer);
        	}
        	
			//Handle a closed connection
	        if (bytesRead == -1) {
	            client.close();
	            System.out.println("\t\tClient disconnected.");
	        } else {
	            // Return their message to them
	            System.out.println("\t\tReceived: " + new String(buffer.array()));
	            // Flip the buffer to now write
	            buffer.flip();
	            
	            String messageHash = hash.SHA1FromBytes(buffer.array());
	            // decide if you want to handle sending a message here or doing it in your threadpoolmanager
	            
	            client.write(buffer);
	            // Clear the buffer
	            buffer.clear();
	            
	            this.throughput.incrementMessageThroughput();
	            this.serverStatistics.incremementServerThroughput();
	            
	            key.interestOps(SelectionKey.OP_READ);
	        }  
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
}

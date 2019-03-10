package cs455.scaling.server;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import cs455.scaling.hash.Hash;

public class Task {

	private static final int BUFFER_SIZE = 8192;
	private final SelectionKey key;
	private final ServerStatistics serverStatistics;
	private final SocketChannel socketChannel;
	private final Hash hash;
	private final Throughput throughput;
	private final ThreadPoolManager threadPoolManager;
	private String message;
	
	public Task(SelectionKey key, ServerStatistics serverStatistics, Hash hash, ThreadPoolManager threadPoolManager) {
		this.key = key;
		this.serverStatistics = serverStatistics;
		this.hash = hash;
		this.socketChannel = (SocketChannel) key.channel();
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
        	// read the whole thing
        	while (readBuffer.hasRemaining() && bytesRead != -1) {
        		bytesRead = channel.read(readBuffer);
        	}
        	
        	readBuffer.rewind();
        	
			//Handle a closed connection
	        if (bytesRead == -1) {
	            channel.close();
	            System.out.println("\t\tClient disconnected.");
	            return;
	        } else {
	            // Return their message to them
	           // System.out.println("\t\tReceived: " + new String(readBuffer.array()));
	            
	            String hashedString = hash.SHA1FromBytes(readBuffer.array());
	            //System.out.println("Got a message from Client that reads: " + hashedString);
	            // decide if you want to handle sending a message here or doing it in your threadpoolmanager
	            
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
		this.threadPoolManager.improvedAddTask(this, key);
	}
	
	public void improvedSendHashResponse() {
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
		
		String clientName = channel.socket().getInetAddress().getHostName();
		int clientPort = channel.socket().getPort();
		
		
		//System.out.println("Sending the String back to the Client: " + paddedResponse);
		//System.out.println("Client is: " + clientName + " on port: " + clientPort);
		
		this.throughput.incrementMessageThroughput();
        this.serverStatistics.incremementServerThroughput();
        
        sendBuffer.clear();
		
	}
	
	public void startTask() {
		// testing moving this into the Task from the Server
		//key.interestOps(SelectionKey.OP_WRITE);
		
		// Create a buffer to read into
        ByteBuffer readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
		int bytesRead = 0;
		readBuffer.clear();

        // Grab the socket from the key 
        SocketChannel channel = (SocketChannel) key.channel();
        
        // Read from it
        try {
        	// read the whole thing
        	while (readBuffer.hasRemaining() && bytesRead != -1) {
        		bytesRead = channel.read(readBuffer);
        	}
        	
        	readBuffer.rewind();
        	
			//Handle a closed connection
	        if (bytesRead == -1) {
	            channel.close();
	            System.out.println("\t\tClient disconnected.");
	            return;
	        } else {
	            // Return their message to them
	           // System.out.println("\t\tReceived: " + new String(readBuffer.array()));
	            
	            String hashedString = hash.SHA1FromBytes(readBuffer.array());
	            System.out.println("Got a message from Client that reads: " + hashedString);
	            // decide if you want to handle sending a message here or doing it in your threadpoolmanager

	            // Clear the buffer
	            readBuffer.clear();
	            
	            //this.throughput.incrementMessageThroughput();
	            //this.serverStatistics.incremementServerThroughput();
	            
	            sendHashResponse(channel, hashedString);
	            
	            //key.interestOps(SelectionKey.OP_READ);
	        }  
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	private void sendHashResponse(SocketChannel channel, String hashedString) {
		// Hash value can be < 40 characters, so we want to pad it to get up to 40, as that's what the client is expecting back
		String paddedResponse = String.format("%40s", hashedString);
		ByteBuffer sendBuffer = ByteBuffer.wrap(paddedResponse.getBytes());
		
		try {
			sendBuffer.rewind();
			
			while (sendBuffer.hasRemaining()) {
				channel.write(sendBuffer);
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		
		System.out.println("Sending the String back to the Client: " + hashedString);
		
		this.throughput.incrementMessageThroughput();
        this.serverStatistics.incremementServerThroughput();
	}
}
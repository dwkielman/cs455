package cs455.scaling.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * A client provides the following functionalities:
 * (1) Connect and maintain an active connection to the server.
 * (2) Regularly send data packets to the server. The payloads for these data packets are 8 KB and the values for these bytes are randomly generated. The rate at which each connection will
 * generate packets is R per-second; include a Thread.sleep(1000/ R) in the client which ensures that you achieve the targeted production rate. The typical value of R is between 2-4.
 * (3) The client should track hashcodes of the data packets that it has sent to the server. A server will acknowledge every packet that it has received by sending the computed hash code back to the client.
 * 
 * Executed with the following command:
 * java cs455.scaling.client.Client server-host server-port message-rate
 */

public class Client {
	
	private final ClientStatistics clientStatistics;
	private final int messageRate;
	private DataInputStream dataInputStream;
	private Socket socket;
	private SenderThread senderThread;
	
	public Client(int messageRate) {
		this.messageRate = messageRate;
		this.clientStatistics = new ClientStatistics();
	}
	
	public static void main(String[] args) {
		
		// requires 1 argument to initialize a Client
		if(args.length != 3) {
		    System.out.println("Invalid Arguments. Must include a Server Host Name, Server Port Number and Message Rate");
		    return;
		}

		String serverHostName = "";
		int serverPortNumber = 0;
		int messageRate = 0;
		
		try {
			serverHostName = args[0];
			serverPortNumber = Integer.parseInt(args[1]);
			messageRate = Integer.parseInt(args[2]);
		} catch (NumberFormatException nfe) {
			System.out.println("Invalid argument(s).");
			nfe.printStackTrace();
		}
		
		Client client = new Client(messageRate);

		try {
			client.connectToServer(serverHostName, serverPortNumber);
			client.clientLoop();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	// set up connection to server
	private void connectToServer(String serverHostName, int serverPortNumber) throws UnknownHostException, IOException {
		socket = new Socket(serverHostName, serverPortNumber);
		dataInputStream = new DataInputStream(socket.getInputStream());
		DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
		startClientStatisticsThread();
		startSenderThread(dataOutputStream);
	}
	
	private void clientLoop() {
		while (true) {
			byte[] incomingData = new byte[40];
			try {
				this.dataInputStream.readFully(incomingData);
				this.senderThread.readHash(new String(incomingData));
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}
	}
	
	// start the thread for displaying client statistics
	private void startClientStatisticsThread() {
		new Thread(clientStatistics).start();
	}
	
	// start the thread for sending messages
	private void startSenderThread(DataOutputStream dataOutputStream) {
		senderThread = new SenderThread(dataOutputStream, messageRate, clientStatistics);
		new Thread(senderThread).start();
	}
}

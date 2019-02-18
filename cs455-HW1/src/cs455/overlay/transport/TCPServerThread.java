package cs455.overlay.transport;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

import cs455.overlay.node.Node;

public class TCPServerThread extends Thread {
	
	private static final boolean DEBUG = false;
	private Node node;
	private ServerSocket ourServerSocket;
	private String hostIPAddress;
	private int portNumber;
	
	
	public TCPServerThread(int portNumber, Node node) {
		try {
			//Create the server socket
			ourServerSocket = new ServerSocket(portNumber);
			this.portNumber = ourServerSocket.getLocalPort();
			this.node = node;
			this.node.setLocalHostPortNumber(ourServerSocket.getLocalPort());
		} catch(IOException e) {
			System.out.println("TCPServerThread::creating_the_socket:: " + e);
			System.exit(1);
		}
	}
	
	public synchronized void run() {
		// get the current host IP address
		try {
			this.hostIPAddress = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException uhe) {
			uhe.printStackTrace();
		}
		
		if (DEBUG) {
			System.out.println("Node is now listening on IP: " + this.hostIPAddress + " Port: " + this.portNumber);
		}
		
		while (!isInterrupted()) {
			try {
				//Block on accepting connections. Once it has received a connection it will return a socket for us to use.
				Socket incomingConnectionSocket = ourServerSocket.accept();
				TCPReceiverThread tcpReceiverThread = new TCPReceiverThread(incomingConnectionSocket, this.node);
				tcpReceiverThread.start();
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("IOException in TCPServerThread");
	            System.exit(1);
			}
		}
	}
}

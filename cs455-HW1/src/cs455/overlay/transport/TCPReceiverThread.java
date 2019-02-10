package cs455.overlay.transport;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

import cs455.overlay.node.Node;
import cs455.overlay.wireformats.EventFactory;

public class TCPReceiverThread implements Runnable {

	private Socket socket;
	private DataInputStream din;
	private Node node;
	private EventFactory eventFactory;
	private static final boolean DEBUG = true;
		
	public TCPReceiverThread(Socket socket, Node node) throws IOException {
		this.node = node;
		this.socket = socket;
		this.eventFactory = EventFactory.getInstance();
		din = new DataInputStream(socket.getInputStream());
	}
		
	@Override
	public void run() {
		
		int dataLength;
		
		while (socket != null) {
			try {
				if (DEBUG) System.out.println("TCPReceiverThread waiting for message length...");
				dataLength = din.readInt();
				if (DEBUG) System.out.println("TCPReceiverThread received message of length: " + dataLength);
				if (DEBUG) System.out.println("TCPReceiverThread awaiting byte array message delivery...");
				byte[] data = new byte[dataLength];
				din.readFully(data, 0, dataLength);
				
				// Notify node of event
				eventFactory.createEvent(data, this.node);
				
			} catch (SocketException se) {
				System.out.println(se.getMessage());
				break;
			} catch (IOException ioe) {
				System.out.println(ioe.getMessage());
				break;
			}
		}
		if (DEBUG) System.out.println("  TCPReceiverThread exiting.");
	}
}

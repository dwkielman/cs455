package cs455.overlay.transport;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

import cs455.overlay.node.Node;
import cs455.overlay.wireformats.EventFactory;

public class TCPReceiverThread extends Thread {

	private Socket socket;
	private DataInputStream din;
	private Node node;
	private EventFactory eventFactory;
		
	public TCPReceiverThread(Socket socket, Node node) throws IOException {
		this.node = node;
		this.socket = socket;
		this.eventFactory = EventFactory.getInstance();
		din = new DataInputStream(socket.getInputStream());
	}
	
	@Override
	public synchronized void run() {
		
		int dataLength;
		
		while (socket != null) {
			try {
				dataLength = din.readInt();
				byte[] data = new byte[dataLength];
				din.readFully(data, 0, dataLength);
				
				// Notify node of event
				eventFactory.createEvent(data, this.node);
				
			} catch (SocketException se) {
				System.out.println("SocketException in TCPReceiverThread");
				//System.out.println(se.getStackTrace());
				System.out.println(se.getMessage());
				break;
			} catch (IOException ioe) {
				System.out.println("IOException in TCPReceiverThread");
				//System.out.println(ioe.getStackTrace());
				System.out.println(ioe.getMessage());
				break;
			}
		}
	}
}

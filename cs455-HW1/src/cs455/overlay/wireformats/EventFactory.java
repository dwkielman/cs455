package cs455.overlay.wireformats;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import cs455.overlay.node.Node;

/**
 * EventFactory is Singleton class that creates instances of Event type that is used to send messages
 */

public class EventFactory {
	
	private static final EventFactory eventFactory = new EventFactory();
	private static final boolean DEBUG = true;
	
	private EventFactory() {};
	
	public static EventFactory getInstance() {
		return eventFactory;
	}
	
	public Event createEvent(byte[] marshalledBytes, Node node) {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		try {
			int type = din.readInt();
			baInputStream.close();
			din.close();
			
			if (DEBUG) {
				System.out.println("Message Type being passed is: " + type);
			}
			
			switch(type) {
				// REGISTER_REQUEST = 6000
				case Protocol.REGISTER_REQUEST:
					return new RegisterRequest(marshalledBytes);
				// REGISTER_RESPONSE = 6001
				case Protocol.REGISTER_RESPONSE:
					return new RegisterResponse(marshalledBytes);	
				// DEREGISTER_REQUEST = 6002
				case Protocol.DEREGISTER_REQUEST:
					return new DeregisterRequest(marshalledBytes);
				// DEREGISTER_RESPONSE = 6003
				case Protocol.DEREGISTER_RESPONSE:
					return new DeregisterResponse(marshalledBytes);
				// MESSAGING_NODES_LIST = 6004
				case Protocol.MESSAGING_NODES_LIST:
					return new MessagingNodesList(marshalledBytes);
				// LINK_WEIGHTS = 6005
				case Protocol.LINK_WEIGHTS:
					return new LinkWeights(marshalledBytes);
				// TASK_INITIATE = 6006
				case Protocol.TASK_INITIATE:
					return new TaskInitiate(marshalledBytes);
				// TASK_COMPLETE = 6007
				case Protocol.TASK_COMPLETE:
					return new TaskComplete(marshalledBytes);
				// PULL_TRAFFIC_SUMMARY = 6008
				case Protocol.PULL_TRAFFIC_SUMMARY:
					return new TaskSummaryRequest(marshalledBytes);
				// TRAFFIC_SUMMARY = 6009
				case Protocol.TRAFFIC_SUMMARY:
					return new TrafficSummaryRequest(marshalledBytes);
				// MESSAGE = 6010
				case Protocol.MESSAGE:
					return new Message(marshalledBytes);
				default:
					System.out.println("Invalid Message Type");
					return null;
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return null;
	}

}

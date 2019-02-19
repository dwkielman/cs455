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
	private static final boolean DEBUG = false;
	
	private EventFactory() {};
	
	public static EventFactory getInstance() {
		return eventFactory;
	}
	
	public synchronized Event createEvent(byte[] marshalledBytes, Node node) {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		Event event = null;
		try {
			int type = din.readInt();
			baInputStream.close();
			din.close();
			
			if (DEBUG) { System.out.println("Message Type being passed is: " + type); }
			
			switch(type) {
				// REGISTER_REQUEST = 6000
				case Protocol.REGISTER_REQUEST:
					event = new RegisterRequest(marshalledBytes);
					break;
				// REGISTER_RESPONSE = 6001
				case Protocol.REGISTER_RESPONSE:
					event = new RegisterResponse(marshalledBytes);	
					break;
				// DEREGISTER_REQUEST = 6002
				case Protocol.DEREGISTER_REQUEST:
					event = new DeregisterRequest(marshalledBytes);
					break;
				// DEREGISTER_RESPONSE = 6003
				case Protocol.DEREGISTER_RESPONSE:
					event = new DeregisterResponse(marshalledBytes);
					break;
				// MESSAGING_NODES_LIST = 6004
				case Protocol.MESSAGING_NODES_LIST:
					event = new MessagingNodesList(marshalledBytes);
					break;
				// LINK_WEIGHTS = 6005
				case Protocol.LINK_WEIGHTS:
					event = new LinkWeights(marshalledBytes);
					break;
				// TASK_INITIATE = 6006
				case Protocol.TASK_INITIATE:
					event = new TaskInitiate(marshalledBytes);
					break;
				// TASK_COMPLETE = 6007
				case Protocol.TASK_COMPLETE:
					event = new TaskComplete(marshalledBytes);
					break;
				// PULL_TRAFFIC_SUMMARY = 6008
				case Protocol.PULL_TRAFFIC_SUMMARY:
					event = new TaskSummaryRequest(marshalledBytes);
					break;
				// TRAFFIC_SUMMARY = 6009
				case Protocol.TRAFFIC_SUMMARY:
					event = new TaskSummaryResponse(marshalledBytes);
					break;
				// MESSAGE = 6010
				case Protocol.MESSAGE:
					event = new Message(marshalledBytes);
					break;
				// NODE_CONNECTION_REQUEST = 6011
				case Protocol.NODE_CONNECTION_REQUEST:
					event = new NodeConnectionRequest(marshalledBytes);
					break;
				// NODE_CONNECTION_RESPONSE = 6012
				case Protocol.NODE_CONNECTION_RESPONSE:
					event = new NodeConnectionResponse(marshalledBytes);
					break;
				default:
					System.out.println("Invalid Message Type");
					return null;
			}
		} catch (IOException ioe) {
			System.out.println("EventFactory Exception");
			ioe.printStackTrace();
		}
		return event;
	}

}

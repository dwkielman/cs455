package cs455.overlay.wireformats;

import java.io.IOException;

/**
 * Once the registry has received TASK_COMPLETE messages from all the registered nodes it will issue a PULL_TRAFFIC_SUMMARY message. This message is sent to all the registered nodes in the system.
 * Message Type: TRAFFIC_SUMMARY
 * Node IP address:
 * Node Port number:
 * Number of messages sent
 * Summation of sent messages
 * Number of messages received
 * Summation of received messages
 * Number of messages relayed
 */

public class TaskSummaryResponse implements Event {

	public TaskSummaryResponse(byte[] marshalledBytes) {
		// TODO Auto-generated constructor stub
	}

	@Override
	public int getType() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public byte[] getBytes() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}

package cs455.overlay.util;

import java.util.ArrayList;

import cs455.overlay.wireformats.TaskSummaryResponse;

public class StatisticsCollectorAndDisplay {

	ArrayList<TaskSummaryResponse> stats;
	// represents the number of messages that were sent by that node
	private int sendTracker;
	// maintains information about the number of messages that were received
	private int receiveTracker;
	// tracking the number of messages that a node relays will depend on the overlay topology, link weights, and shortest paths that it belongs to
	private int relayTracker;
	// track the messages that it has sent. continuously sums the values of the random numbers that are sent
	private long sendSummation;
	// track the messages that it has received. sums values of the payloads that are received
	private long receiveSummation;
	private int nodes;
	
	public StatisticsCollectorAndDisplay(int nodes) {
		this.stats = new ArrayList<>();
		this.sendTracker = 0;
		this.receiveTracker = 0;
		this.relayTracker = 0;
		this.sendSummation = 0;
		this.receiveSummation = 0;
		this.nodes = nodes;
	}
	
	public void addTrafficSummary(TaskSummaryResponse taskSummaryResponse) {
		this.stats.add(taskSummaryResponse);
		this.sendTracker += taskSummaryResponse.getNumberOfMessagesSent();
		this.receiveTracker += taskSummaryResponse.getNumberOfMessagesReceived();
		this.relayTracker += taskSummaryResponse.getNumberOfMessagesRelayed();
		this.sendSummation += taskSummaryResponse.getSumReceivedMessages();
		this.receiveSummation += taskSummaryResponse.getSumReceivedMessages();
		
		if (this.stats.size() == this.nodes) {
			displayStatistics();
		}
		
	}
	
	// display the stats in a user-friendly format as defined by the requirements
	private void displayStatistics() {
		System.out.println("Node\tNumber of messages sent\tNumber of messages redceived\tSumnmation of sent messages\tSummation of received messages\tNumber of messaged relayed");
		int nodeNum = 1;
		for (TaskSummaryResponse tsr : stats) {
			String display = "";
			display += "Node " + nodeNum + "\t";
			display += tsr.getNumberOfMessagesSent() + "\t";
			display += tsr.getNumberOfMessagesReceived() + "\t";
			display += tsr.getSumSentMessages() + "\t";
			display += tsr.getSumReceivedMessages() + "\t";
			display += tsr.getNumberOfMessagesRelayed() + "\t";
			System.out.println(display);
			nodeNum++;
		}
		String summary = "";
		summary += "Sum\t";
		summary += this.sendTracker + "\t";
		summary += this.receiveTracker + "\t";
		summary += this.relayTracker + "\t";
		summary += this.sendSummation + "\t";
		summary += this.receiveSummation;
		System.out.println(summary);
	}
	
}

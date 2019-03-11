package cs455.scaling.server;

import java.nio.channels.SelectionKey;
import java.time.LocalDateTime;
import java.util.LinkedList;

/**
 * The server relies on the thread pool to perform all tasks. The threads within the thread pool should be, created just once.
 * Care must be taken to ensure that you are not inadvertently creating a new thread every time a task needs to be performed
 *
 */

public class ThreadPoolManager implements Runnable {

	// Each unit of work is a list of data packets with a maximum length of batch-size
	private final LinkedList<Task> tasksToPerformList;
	private final int batchSize;
	private final int batchTime;
	private final ThreadPool threadPool;
	
	public ThreadPoolManager(int threadPoolSize, int batchSize, int batchTime) {
		this.batchSize = batchSize;
		this.batchTime = batchTime;
		this.tasksToPerformList = new LinkedList<Task>();
		this.threadPool = new ThreadPool(threadPoolSize);
		this.threadPool.startWorkerThreads();
	}
	
	// Work units are added to the tail of the work queue 
	public void addTask(Task task, SelectionKey key) {
		synchronized(tasksToPerformList) {
			this.tasksToPerformList.add(task);
		}
	}
	
	// assign tasks to the available worker threads until the list is empty
	private void assignTaskToWorkerThread() {
		synchronized (tasksToPerformList) {
			while(!tasksToPerformList.isEmpty()) {
				WorkerThread worker = threadPool.getSpareWorkerThread();
				Task newTask = tasksToPerformList.poll();
				if (newTask == null) {
					//System.out.println("No Tasks available to assign yet, waiting for a Task to be added to the ThreadPoolManager.");
					try {
						tasksToPerformList.wait();
						newTask = tasksToPerformList.removeFirst();
					} catch (InterruptedException ie) {
						ie.printStackTrace();
					}
				}
				worker.assignTaskToWorkerThread(newTask);
			}
		}
	}

	public boolean checkForSpareTasks() {
		synchronized(tasksToPerformList) {
			return this.tasksToPerformList.isEmpty();
		}
	}
	
	private synchronized void clearTasksToPerformList() {
		this.tasksToPerformList.clear();
	}

	// When the work unit at the top of the queue has either:
	// (1) reached a length of batch-size or
	// (2) batch-time has expired since the previous unit was removed, an available worker is assigned to the work unit.
	@Override
	public void run() {
		// create the time we will wait until
		LocalDateTime messageTime = LocalDateTime.now().plusSeconds(batchTime);
		
		while (true) {
			// get the current time
			LocalDateTime now = LocalDateTime.now();
			// start assigning tasks only after the batch time has expired since previous batch was removed or batch size has reached maximum size
			 synchronized (tasksToPerformList) {
				if (now.isAfter(messageTime) || this.tasksToPerformList.size() >= this.batchSize) {
						//System.out.println("Time elapsed or Batch Size met, beginning Tasks.");
						messageTime = now.plusSeconds(batchTime);
						if (!this.tasksToPerformList.isEmpty()) {
							assignTaskToWorkerThread();
							clearTasksToPerformList();
						} else {
							System.out.println("No Tasks to assign, timer is resetting.");
						}
						//System.out.println("Restarting ThreadPoolManagerThread, tasks should be 0: " + this.tasksToPerformList.size());
				}
			}
		}
	}
}
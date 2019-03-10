package cs455.scaling.server;

import java.nio.channels.SelectionKey;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * The server relies on the thread pool to perform all tasks. The threads within the thread pool should be, created just once.
 * Care must be taken to ensure that you are not inadvertently creating a new thread every time a task needs to be performed
 *
 */
	// Each unit of work is a list of data packets with a maximum length of batch-size

public class ThreadPoolManager implements Runnable {

	private final LinkedList<Task> tasksToPerformList;
	private final int threadPoolSize;
	private final int batchSize;
	private final int batchTime;
	private final ThreadPool threadPool;
	private final Object lock = new Object();
	private final Object assignLock = new Object();
	
	public ThreadPoolManager(int threadPoolSize, int batchSize, int batchTime) {
		this.threadPoolSize = threadPoolSize;
		this.batchSize = batchSize;
		this.batchTime = batchTime;
		this.tasksToPerformList = new LinkedList<Task>();
		this.threadPool = new ThreadPool(threadPoolSize);
		this.threadPool.startWorkerThreads();
	}
	
	public void improvedAddTask(Task task, SelectionKey key) {
		synchronized(tasksToPerformList) {
			this.tasksToPerformList.add(task);
			//System.out.println("Task added through improved method to the list! Size is currently: " + this.tasksToPerformList.size());
		}
	}
	
	private void improvedAssignTaskToWorkerThread() {
		//synchronized(tasksToPerformList) {
			// Work units are added to the tail of the work queue and when the work unit at the top of the queue has either:
			// (1) reached a length of batch-size or
			// (2) batch-time has expired since the previous unit was removed, an available worker is assigned to the work unit.
		synchronized (tasksToPerformList) {
			while(!tasksToPerformList.isEmpty()) {
				//System.out.println("Number of tasks to assign in TPM: " + tasksToPerformList.size());
				WorkerThread worker = threadPool.getSpareWorkerThread();
				Task newTask = tasksToPerformList.poll();
				if (newTask == null) {
					System.out.println("No Tasks available to assign yet, waiting for a Task to be added to the ThreadPoolManager.");
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

	@Override
	public void run() {
		// Create the time we will wait until
		LocalDateTime messageTime = LocalDateTime.now().plusSeconds(batchTime);
		
		while (true) {
			// get the current time
			LocalDateTime now = LocalDateTime.now();
			// start assigning tasks only after the batch time has expired since previous batch was removed or batch size has reached maximum size
			 synchronized (tasksToPerformList) {
				if (now.isAfter(messageTime) || this.tasksToPerformList.size() >= this.batchSize) {
					synchronized (lock) {
						System.out.println("Time elapsed or Batch Size met, beginning Tasks.");
						messageTime = now.plusSeconds(batchTime);
						if (!this.tasksToPerformList.isEmpty()) {
							//assignTaskToWorkerThread(this.tasksToPerformList);
							improvedAssignTaskToWorkerThread();
							
							//LinkedList<Task> oldTasksToPerformList = new LinkedList<Task>(this.tasksToPerformList);
							clearTasksToPerformList();
							//assignTaskToWorkerThread(oldTasksToPerformList);
						} else {
							System.out.println("No Tasks to assign, timer is resetting.");
						}
						System.out.println("Restarting ThreadPoolManagerThread, tasks should be 0: " + this.tasksToPerformList.size());
					}
				}
			}
			
		}
	}
}
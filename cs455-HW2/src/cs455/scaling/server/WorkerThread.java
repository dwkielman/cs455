package cs455.scaling.server;

public class WorkerThread implements Runnable {
	
	private ThreadPool threadPool;
	private boolean isDoneWithTask = false;
	private Task currentTask;
	
	public WorkerThread(ThreadPool threadPool) {
		this.threadPool = threadPool;
		this.currentTask = null;
	}
	
	public synchronized void assignTaskToWorkerThread(Task task) {
		if (task != null) {
			this.currentTask = task;
			isDoneWithTask = false;
			notify();
		}
	}
	
	@Override
	public void run() {
		while(true) {
			synchronized(this) {
				if(currentTask != null) {
					currentTask.run();
					isDoneWithTask = true;
					currentTask = null;
					threadPool.addWorkerThreadBackToPool(this);
				}
			}
		}
	}
}

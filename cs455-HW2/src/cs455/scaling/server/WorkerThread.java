package cs455.scaling.server;

public class WorkerThread implements Runnable {
	
	private ThreadPool threadPool;
	private Task currentTask;
	
	public WorkerThread(ThreadPool threadPool) {
		this.threadPool = threadPool;
		this.currentTask = null;
	}
	
	public synchronized void assignTaskToWorkerThread(Task task) {
		if (task != null) {
			this.currentTask = task;
			notify();
		}
	}
	
	@Override
	public void run() {
		while(true) {
			synchronized(this) {
				if(currentTask != null) {
					currentTask.sendHashResponse();
					currentTask = null;
					threadPool.addWorkerThreadBackToPool(this);
				}
			}
		}
	}
}

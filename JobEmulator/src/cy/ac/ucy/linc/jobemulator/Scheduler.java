package cy.ac.ucy.linc.jobemulator;

import java.util.Timer;
import java.util.TimerTask;

import cy.ac.ucy.linc.jobemulator.job.IJob;

/**
 * From JCatascopia Monitoring System
 * 
 * https://github.com/dtrihinas/JCatascopia 
 * 
 **/
public class Scheduler {
	
	private Timer scheduler;
	
	public Scheduler() {
		this.scheduler = new Timer();
	}
	
	public void scheduleTask(IJob job, long period) {
		scheduler.schedule((TimerTask) job, period, period);
	}
	
	public void terminate() {
		this.scheduler.cancel();
	}
}
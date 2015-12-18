package cy.ac.ucy.linc.jobemulator.job;

import java.util.TimerTask;
import java.util.UUID;

public abstract class Job extends TimerTask implements IJob {
	
	private String jobID;

	public Job() {
		this.jobID = UUID.randomUUID().toString();
	}
	
	public String getJobID() {
		return this.jobID;
	}
		
	public void run() {
		doWork();
	}
	
	public abstract void doWork();
	
}
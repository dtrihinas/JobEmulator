package cy.ac.ucy.linc.jobemulator.job.library;

import cy.ac.ucy.linc.jobemulator.job.Job;

public class DummyJob extends Job {

	@Override
	public void doWork() {
		System.out.println("DummyJob with id: " + this.getJobID() + " says hello!");
	}
}

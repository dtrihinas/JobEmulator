package cy.ac.ucy.linc.jobemulator;
import java.util.Random;

import cy.ac.ucy.linc.jobemulator.job.IJob;
import cy.ac.ucy.linc.jobemulator.utils.DynamicInstanceLoader;

public class JobEmulator {

	private static final String ARGS_MSG = "JobEmulator <job_num> <job_period> <job_type> [<job_path>]";
	
	private static final String DEFAULT_JOB_PATH = "cy.ac.ucy.linc.jobemulator.job.library";
	private static final boolean DEFAULT_ARTIFICIAL_DELAY_FLAG = true;
	private static final int DEFAULT_ARTIFICIAL_DELAY = 3;
	
	private int jobNum;
	private long jobPeriod;
	private String jobPath;
	private String jobType;
	private boolean delay;
	
	private Scheduler scheduler; 
		
	public JobEmulator(int jobNum, long jobPeriod, String jobType, String jobPath) {
		this.jobNum = jobNum;
		this.jobPeriod = jobPeriod; //in milliseconds
		this.jobPath = jobPath;
		this.jobType = jobType;
		
		this.scheduler = new Scheduler();
		
		this.delay = JobEmulator.DEFAULT_ARTIFICIAL_DELAY_FLAG;
		System.out.println("JobEmulator>> emulator enabled with (" + jobNum + ") jobs, period (" + jobPeriod + ") ms and type (" + jobType + ")");
	}
	
	public JobEmulator(int jobNum, long jobPeriod, String jobType) {
		this(jobNum, jobPeriod, jobType, JobEmulator.DEFAULT_JOB_PATH);
	}
	
	public void start() {
		for(int i=0; i<this.jobNum; i++) {
			try {
				IJob j = instaJob();
				if (this.delay)
					JobEmulator.addArtificialDelay();
				this.scheduler.scheduleTask(j, this.jobPeriod);
			}
			catch(Exception e) {
				e.printStackTrace();
				//TODO when taking different job types do not exist but stop scheduling failed task... 
				//now if one fails the rest will of course too...
				System.exit(1); 
			}
		}
	}
	
	private IJob instaJob() throws Exception {
		return DynamicInstanceLoader.newInstance(this.jobPath, this.jobType);
	}
	
	private static void addArtificialDelay() {
		try {
			Random r = new Random();
			long i = r.nextInt(JobEmulator.DEFAULT_ARTIFICIAL_DELAY) * 1000;
			Thread.sleep(i);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		try {
			if (args.length < 3 || args.length > 4)
				errorMsg();
			
			int jobNum = Integer.parseInt(args[0]);
			long jobPeriod = Long.parseLong(args[1]);
			String jobType = args[2];
			String jobPath = null;
			
			JobEmulator emulator;
			if (args.length == 4)				
				 emulator = new JobEmulator(jobNum, jobPeriod, jobType, jobPath);
			else
				emulator = new JobEmulator(jobNum, jobPeriod, jobType);
			emulator.start();
		}
		catch(Exception e) {
			e.printStackTrace();
			errorMsg();
		}
	}
	
	private static void errorMsg() {
		System.out.println(JobEmulator.ARGS_MSG);
		System.exit(1);
	}
}

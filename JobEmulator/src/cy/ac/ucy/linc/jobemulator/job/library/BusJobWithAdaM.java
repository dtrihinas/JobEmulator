package cy.ac.ucy.linc.jobemulator.job.library;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import cy.ac.ucy.linc.adam.sampler.Sample;
import cy.ac.ucy.linc.adam.sampler.AdaM.AdaM;
import cy.ac.ucy.linc.jobemulator.job.Job;

public class BusJobWithAdaM extends Job {

	//TODO use a config file
	private static final String DEFAULT_SERVER_PORT = "9092";
	private static final String DEFAULT_SERVER_IP = "10.16.3.177";
//	private static final String DEFAULT_SERVER_IP = "192.168.0.201";
	private static final String DEFAULT_DATASET_PATH = "/home/dtrihinas/buses";
//	private static final String DEFAULT_DATASET_PATH = "/home/ubuntu/buses";
	private static final String DEFAULT_KAFKA_TOPIC = "buses";
	
	private String busID;
	private String serverIP;
	private String serverPort;
	private String datasetPath;

	private BufferedReader dataset;
	private BufferedWriter output;
	
	Producer<String,String> distributor;
	
	private AdaM adam;
	private long index;
	private int i;
	private int adamperiod;
	
	public BusJobWithAdaM() {
		HashMap<String,String> config =  parseConfig();
		this.serverIP = config.get("serverIP");
		this.serverPort = config.get("serverPort");
		this.datasetPath = config.get("dataset");
		
		//this.busID = BusJobWithAdaM.getBusID(this.datasetPath);
		this.busID = "13";
		System.out.println(this.busID);
		
		this.adam = new AdaM(1, 10, 0.15, 1, 0, 0);
		this.index = 0L;
		this.i = 0;
		this.adamperiod = 0;
		
		try {
			this.dataset = new BufferedReader(new FileReader(this.datasetPath + File.separator + this.busID));
			this.output = new BufferedWriter(new FileWriter("output.csv"));
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
		this.distributor = BusJobWithAdaM.configKafka(this.serverIP, this.serverPort);

	}
	
	@Override
	public void doWork() {
		String event;
		try {
			if ((event = this.dataset.readLine()) != null) {
				String[] tmp = event.split(",");
				StringBuffer msg = new StringBuffer();
				if (tmp.length < 12) {
					return;
				}
				msg.append("{");
				msg.append("\"timestamp\":\"" + tmp[0] + "\",");
				msg.append("\"lineID\":\"" + tmp[1] + "\",");
				msg.append("\"direction\":\"" + tmp[2] + "\",");
				msg.append("\"journeyID\":\"" + tmp[3] + "\",");
				msg.append("\"timeframe\":\"" + tmp[4] + "\",");
				msg.append("\"vehiclejourneyID\":\"" + tmp[5] + "\",");
				msg.append("\"operator\":\"" + tmp[6] + "\",");
				msg.append("\"congestion\":\"" + tmp[7] + "\",");
				msg.append("\"delay\":\"" + tmp[8] + "\",");
				msg.append("\"blockID\":\"" + tmp[9] + "\",");
				msg.append("\"vehicleID\":\"" + tmp[10] + "\",");
				msg.append("\"stopID\":\"" + tmp[11] + "\",");
				msg.append("\"atStop\":\"" + tmp[12] + "\"");
				msg.append("}");
				
				if (this.i == this.adamperiod) {
					this.adam.feedSample(new Sample(this.index, Double.parseDouble(tmp[8])));
					this.adamperiod = this.adam.getSamplingPeriod();
					this.i = 0;
					this.index += this.adamperiod;
				
					//System.out.println("new period: " + this.adamperiod + ", data index: " + this.index + ", curVal: " + tmp[8]);
					Date d = new java.util.Date(Long.parseLong(tmp[0])/1000);
					String s = d.toGMTString() + "," + this.adamperiod + "," + this.index;
					System.out.println(s);
					this.output.write(s + "\n");
					this.output.flush();
					
					//this.distributeEvent(msg.toString());
				}
				else 
					this.i++;
			}
			else {
				//reset stream
				this.dataset = new BufferedReader(new FileReader(this.datasetPath + File.separator + this.busID));
				//doWork();
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void distributeEvent(String event) {
		System.out.println(event);
		this.distributor.send(new KeyedMessage<String, String>(DEFAULT_KAFKA_TOPIC, event));
	}
	
	private static Producer<String, String> configKafka(String ip, String port) {
		Properties properties = new Properties();
        properties.put("metadata.broker.list",ip + ":" + port);
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        ProducerConfig producerConfig = new ProducerConfig(properties);
    	return new Producer<String, String>(producerConfig);
	}
	
	private HashMap<String,String> parseConfig() {
		HashMap<String,String> config = new HashMap<String,String>();
		//TODO read from an actual config file
		//for now just use defaults;
		config.put("serverIP", BusJobWithAdaM.DEFAULT_SERVER_IP);
		config.put("serverPort", BusJobWithAdaM.DEFAULT_SERVER_PORT);
		config.put("dataset", BusJobWithAdaM.DEFAULT_DATASET_PATH);
		
		return config;
	}
	
	private static String getBusID(String path) {
		File dir = new File(path);
		File[] list = dir.listFiles();
		Random r = new Random();
		return list[r.nextInt(list.length-1)].getName();
	}
}

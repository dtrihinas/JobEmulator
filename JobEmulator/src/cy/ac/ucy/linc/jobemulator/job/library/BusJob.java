package cy.ac.ucy.linc.jobemulator.job.library;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import cy.ac.ucy.linc.jobemulator.job.Job;

public class BusJob extends Job {

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
	
	Producer<String,String> distributor;
	
	public BusJob() {
		HashMap<String,String> config =  parseConfig();
		this.serverIP = config.get("serverIP");
		this.serverPort = config.get("serverPort");
		this.datasetPath = config.get("dataset");
		
		this.busID = BusJob.getBusID(this.datasetPath);
		
		try {
			this.dataset = new BufferedReader(new FileReader(this.datasetPath + File.separator + this.busID));
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
		this.distributor = BusJob.configKafka(this.serverIP, this.serverPort);

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
				
				this.distributeEvent(msg.toString());
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
		config.put("serverIP", BusJob.DEFAULT_SERVER_IP);
		config.put("serverPort", BusJob.DEFAULT_SERVER_PORT);
		config.put("dataset", BusJob.DEFAULT_DATASET_PATH);
		
		return config;
	}
	
	private static String getBusID(String path) {
		File dir = new File(path);
		File[] list = dir.listFiles();
		Random r = new Random();
		return list[r.nextInt(list.length-1)].getName();
	}
}

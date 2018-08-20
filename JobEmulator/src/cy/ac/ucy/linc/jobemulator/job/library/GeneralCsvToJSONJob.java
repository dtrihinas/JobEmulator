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

public class GeneralCsvToJSONJob extends Job {

    //TODO use a config file
    private static final String DEFAULT_SERVER_PORT = "9094";
    private static final String DEFAULT_SERVER_IP = "localhost";
    //	private static final String DEFAULT_SERVER_IP = "192.168.0.201";
    private static final String DEFAULT_DATASET_PATH = "/home/ubuntu/taxis";
    //	private static final String DEFAULT_DATASET_PATH = "/home/ubuntu/buses";
    private static final String DEFAULT_KAFKA_TOPIC = "buses";

    private String id;
    private String serverIP;
    private String serverPort;
    private String datasetPath;
    private String[] fields;
    boolean firstLine = true;

    private BufferedReader dataset;

    Producer<String,String> distributor;

    public GeneralCsvToJSONJob() {
        HashMap<String,String> config =  parseConfig();
        this.serverIP = config.get("serverIP");
        this.serverPort = config.get("serverPort");
        this.datasetPath = config.get("dataset");

        this.id = GeneralCsvToJSONJob.getId(this.datasetPath);

        try {
            this.dataset = new BufferedReader(new FileReader(this.datasetPath + File.separator + this.id));
        }
        catch(Exception e) {
            e.printStackTrace();
        }

        this.distributor = GeneralCsvToJSONJob.configKafka(this.serverIP, this.serverPort);

    }

    @Override
    public void doWork() {
        String event;
        try {

            if ((event = this.dataset.readLine()) != null) {
                String[] tmp = event.split(",");
                StringBuffer msg = new StringBuffer();

                if (firstLine){
                    this.fields = tmp;
                    System.out.println(this.fields.length);
                    this.firstLine = false;
                }else{

                    if (tmp.length < this.fields.length) {
                        return;
                    }


                    msg.append("{");
                    for (int i=0; i<this.fields.length; i++){
                        if (i< this.fields.length - 1){

                            msg.append("\""+this.fields[i]+"\":\"" + tmp[i] + "\",");
                        }else{
                            msg.append("\""+this.fields[i]+"\":\"" + tmp[i] + "\"");
                        }

                    }
                    msg.append("}");

                    this.distributeEvent(msg.toString());

                }


            }
            else {
                //reset stream
                this.dataset = new BufferedReader(new FileReader(this.datasetPath + File.separator + this.id));
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
        config.put("serverIP", GeneralCsvToJSONJob.DEFAULT_SERVER_IP);
        config.put("serverPort", GeneralCsvToJSONJob.DEFAULT_SERVER_PORT);
        config.put("dataset", GeneralCsvToJSONJob.DEFAULT_DATASET_PATH);

        return config;
    }

    private static String getId(String path) {
        System.out.println(path);
        File dir = new File(path);
        File[] list = dir.listFiles();
        System.out.println(list.length);
        Random r = new Random();
        System.out.println(list.length);
        if (list.length>1){
            return list[r.nextInt(list.length-1)].getName();
        }else if (list.length == 1){
            return list[0].getName();
        }else{
            return "error the folder is empty";
        }

    }
}
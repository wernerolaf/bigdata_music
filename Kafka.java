import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Arrays;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class Main {

	public static void main(String[] args) throws IOException {
		
		if(args.length < 2){
	         System.out.println("Usage: consumer <topic> <groupname>");
	         return;
	      }
		int chunk_size=60;
	      String topic = args[0].toString();
	      String group = args[1].toString();
	      Properties props = new Properties();
	      props.put("bootstrap.servers", "localhost:9092");
	      props.put("group.id", group);
	      props.put("enable.auto.commit", "true");
	      props.put("auto.commit.interval.ms", "1000");
	      props.put("session.timeout.ms", "30000");
	      props.put("key.deserializer",          
	         "org.apache.kafka.common.serialization.ByteArraySerializer");
	      props.put("value.deserializer", 
	         "org.apache.kafka.common.serializa-tion.StringDeserializer");
	      KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
	      
	      consumer.subscribe(Arrays.asList(topic));
	      System.out.println("Subscribed to topic " + topic);
	      String[] list=new String[60];
	      int j;
	      while (true) {
	    	  j=0;
	         ConsumerRecords<String, String> records = consumer.poll(60);
	            for (ConsumerRecord<String, String> record : records) {
	               System.out.printf("offset = %d, key = %s, value = %s\n", 
	               record.offset(), record.key(), record.value());
	               list[j]="\"" + record.value() + "\"";
	               j+=1;
	            }
		try {
				String converted = String.join(" ", list);
			
			
				TimeUnit.SECONDS.sleep(1);
				System.out.println(converted);
				String query = "Select Distinct ?human ?id ?knownAs ?age ?gender ?genre ?instrument ?pseudonym ?birth ?death ?birthplace\n"
						+ "Where{\n" + "  #Q483501\n" + "  VALUES ?id {" + converted + "}\n"
						+ "?human wdt:P31 wd:Q5;\n"
						+ "         wdt:P1902 ?id;\n"
						+ "         wdt:P27 ?country;\n"
						+ "         wdt:P106 ?occupation.\n"
						+ "  ?occupation wdt:P279* wd:Q483501.\n"
						+ "  \n"
						+ "  Optional{?human rdfs:label ?knownAs.\n"
						+ "          FILTER(lang(?knownAs) = \"en\")\n"
						+ "        }\n"
						+ "  \n"
						+ "  Optional{?human wdt:P21 ?gender2.\n"
						+ "          ?gender2 rdfs:label ?gender.\n"
						+ "           FILTER(lang(?gender) = \"en\")\n"
						+ "        }\n"
						+ "  optional{?human wdt:P1303 ?instrument2.\n"
						+ "          ?instrument2 rdfs:label ?instrument.\n"
						+ "          FILTER(lang(?instrument) = \"en\")}\n"
						+ "  optional{?human wdt:P742 ?pseudonym.}\n"
						+ "  optional{?human wdt:P19 ?birthplace2.\n"
						+ "          ?birthplace2 rdfs:label ?birthplace.\n"
						+ "          FILTER(lang(?birthplace) = \"en\")}\n"
						+ "  optional{?human wdt:P136 ?genre2.\n"
						+ "          ?genre2 rdfs:label ?genre. \n"
						+ "          FILTER(lang(?genre) = \"en\")}\n"
						+ "  optional{?human wdt:P569 ?birth.}\n"
						+ "  optional{?human wdt:P570 ?death.}\n"
						+ "      BIND(IF(Bound(?death),YEAR(xsd:dateTime(?death))-YEAR(xsd:dateTime(?birth)),YEAR(NOW())-YEAR(xsd:dateTime(?birth))) AS ?age)\n"
						+ "  }";
				String result;
				result = get_wikidata(query);
				
			
		} catch (Exception e) {
			System.out.println(e);
		}}
	}

	public static <T> T[] subArray(T[] array, int beg, int end) {
		return Arrays.copyOfRange(array, beg, Math.min(end,array.length));
	}
	
	public static String joinNonBlankStringArray(String s[], String separator) {
	    StringBuilder sb = new StringBuilder();
	    if (s != null && s.length > 0) {
	        for (String w : s) {
	            if (w != null && !w.trim().isEmpty()) {
	                sb.append(w);
	                sb.append(separator);
	            }
	        }
	    }
	    return sb.substring(0, sb.length() - 1);
	}
	
	public static String merge(String[] results) {
		String merged;
		merged=results[0].split("\"bindings\"")[0]+"\"bindings\" : [";
		for (int i = 0; i < results.length; i++) {
			String tmp=results[i].split("\"bindings\"")[1];
			results[i]=tmp.substring(4, tmp.lastIndexOf("]")-1);
		}
		merged=merged+joinNonBlankStringArray(results,",")+" ]\n"
				+ "  }\n"
				+ "}";
		return(merged);
				}
	
	public static String open(String fileName) throws IOException {
		Scanner s;
		String result;
		System.out.println("\n -- OPEN FILE --");
		URL url = new URL(
				"http://localhost:50070/webhdfs/v1/user/bigdata_music/" + fileName + "?user.name=hdfs&op=OPEN");
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		connection.setRequestMethod("GET");
		connection.setDoInput(true);
		s = new Scanner(connection.getInputStream()).useDelimiter("\\A");
		try {
			result = s.hasNext() ? s.next() : "";

			return (result);
		} finally {
			s.close();
		}
	}

	public static String get_wikidata(String query) throws IOException {
		Scanner s;
		String result;
		System.out.println("\n -- CONNECT --");
		URL url = new URL("https://query.wikidata.org/sparql?query=" + URLEncoder.encode(query, "UTF-8"));
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		connection.setRequestMethod("GET");
		connection.setDoInput(true);
		connection.setRequestProperty("Accept", "application/sparql-results+json");
		s = new Scanner(connection.getInputStream()).useDelimiter("\\A");
		try {
			result = s.hasNext() ? s.next() : "";
			return result;
		} finally {
			if (!s.equals(null)) {
				s.close();
			}
		}
	}

	public static void send_json(String fileName, String fileText) throws IOException {
		System.out.println("\n -- CREATE FILE --");
		URL url = new URL(
				"http://localhost:50070/webhdfs/v1/user/bigdata_music/" + fileName + "?user.name=hdfs&op=CREATE");
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		connection.setRequestMethod("PUT");
		connection.setDoInput(true);
		connection.setDoOutput(true);
		connection.getOutputStream().write(fileText.getBytes());
		Map<String, List<String>> header = connection.getHeaderFields();
		for (String field : header.keySet())
			System.out.println(field + ": " + header.get(field));
	}

}

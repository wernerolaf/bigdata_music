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
//import simple producer packages
import org.apache.kafka.clients.producer.Producer;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;

public class Kafka {
    private static final Logger logger = LogManager.getLogger(Kafka.class);

    public static void main(String[] args) throws IOException {

        // Assign topicName to string variable
        String topicName = "wikidata";

        // create instance for properties to access producer configs
        Properties props = new Properties();

        // Assign localhost id
        props.put("bootstrap.servers", "sandbox.hortonworks.com:6667");

        // Set acknowledgements for producer requests.
        props.put("acks", "all");

        // If the request fails, the producer can automatically retry,
        props.put("retries", 0);

        // Specify buffer size in config
        props.put("batch.size", 16384);

        // Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        // The buffer.memory controls the total amount of memory available to the
        // producer for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        final int chunk_size = 60;
        String topic = "spotify_ids";
        String group = "group1";
        Properties props2 = new Properties();
        props2.put("bootstrap.servers", "sandbox.hortonworks.com:6667");
        props2.put("group.id", group);
        props2.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props2.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props2);

        consumer.subscribe(Arrays.asList(topic));
        System.out.println("Subscribed to topic " + topic);
        String[] list = new String[chunk_size];
        int j = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());

                list[j] = "\"" + record.value() + "\"";
                j += 1;
                if(j==chunk_size){
                    j=0;

                    try {
                        String converted = joinNonBlank(" ", list);

                        TimeUnit.SECONDS.sleep(1);
                        System.out.println(converted);
                        String query = "Select Distinct ?human ?id ?knownAs ?age ?gender ?genre ?instrument ?pseudonym ?birth ?death ?birthplace\n"
                                + "Where{\n" + "  #Q483501\n" + "  VALUES ?id {" + converted + "}\n" + "?human wdt:P31 wd:Q5;\n"
                                + "         wdt:P1902 ?id;\n" + "         wdt:P27 ?country;\n"
                                + "         wdt:P106 ?occupation.\n" + "  ?occupation wdt:P279* wd:Q483501.\n" + "  \n"
                                + "  Optional{?human rdfs:label ?knownAs.\n" + "          FILTER(lang(?knownAs) = \"en\")\n"
                                + "        }\n" + "  \n" + "  Optional{?human wdt:P21 ?gender2.\n"
                                + "          ?gender2 rdfs:label ?gender.\n" + "           FILTER(lang(?gender) = \"en\")\n"
                                + "        }\n" + "  optional{?human wdt:P1303 ?instrument2.\n"
                                + "          ?instrument2 rdfs:label ?instrument.\n"
                                + "          FILTER(lang(?instrument) = \"en\")}\n"
                                + "  optional{?human wdt:P742 ?pseudonym.}\n" + "  optional{?human wdt:P19 ?birthplace2.\n"
                                + "          ?birthplace2 rdfs:label ?birthplace.\n"
                                + "          FILTER(lang(?birthplace) = \"en\")}\n" + "  optional{?human wdt:P136 ?genre2.\n"
                                + "          ?genre2 rdfs:label ?genre. \n" + "          FILTER(lang(?genre) = \"en\")}\n"
                                + "  optional{?human wdt:P569 ?birth.}\n" + "  optional{?human wdt:P570 ?death.}\n"
                                + "      BIND(IF(Bound(?death),YEAR(xsd:dateTime(?death))-YEAR(xsd:dateTime(?birth)),YEAR(NOW())-YEAR(xsd:dateTime(?birth))) AS ?age)\n"
                                + "  }";
                        String result;
                        result = get_wikidata(query);
                        producer.send(new ProducerRecord<String, String>(topicName, list[0], result));


                    } catch (Exception e) {
                        System.out.println(e);
                        consumer.close();
                        producer.close();
                    }}
            }

        }
    }

    public static <T> T[] subArray(T[] array, int beg, int end) {
        return Arrays.copyOfRange(array, beg, Math.min(end, array.length));
    }

    public static String joinNonBlank(String separator, String s[]) {
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
        merged = results[0].split("\"bindings\"")[0] + "\"bindings\" : [";
        for (int i = 0; i < results.length; i++) {
            String tmp = results[i].split("\"bindings\"")[1];
            results[i] = tmp.substring(4, tmp.lastIndexOf("]") - 1);
        }
        merged = merged + joinNonBlank(",",results) + " ]\n" + "  }\n" + "}";
        return (merged);
    }



    public static String get_wikidata(String query) throws IOException {
        Scanner s;
        String result;
        System.out.println("\n -- CONNECT --");
        URL url = new URL("https://query.wikidata.org/sparql?query=" + URLEncoder.encode(query, "UTF-8"));
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setDoInput(true);
        connection.setRequestProperty("Accept", "text/csv");
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
}

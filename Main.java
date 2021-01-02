import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) throws IOException {
    	String query="Select Distinct ?human ?id ?knownAs ?age ?gender ?genre ?instrument ?pseudonym ?birth ?death ?birthplace\n"
    			+ "Where{\n"
    			+ "  #Q483501\n"
    			+ "  VALUES ?country {wd:Q36}\n"
    			+ "  ?human wdt:P31 wd:Q5;\n"
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
    			+ "  \n"
    			+ "  #Optional{?human wdt:P735 ?name2.\n"
    			+ "  #       ?name2 rdfs:label ?name.\n"
    			+ "  #      FILTER(lang(?name) = \"en\")\n"
    			+ "  #     }\n"
    			+ "  \n"
    			+ "  #Optional{?human wdt:P734 ?surname2.\n"
    			+ "  #       ?surname2 rdfs:label ?surname.\n"
    			+ "  #       FILTER(lang(?surname) = \"en\")\n"
    			+ "  #     }\n"
    			+ "  optional{?human wdt:P1303 ?instrument2.\n"
    			+ "          ?instrument2 rdfs:label ?instrument.\n"
    			+ "          FILTER(lang(?instrument) = \"en\")}\n"
    			+ "  optional{?human wdt:P742 ?pseudonym.}\n"
    			+ "  optional{?human wdt:P19 ?birthplace2.\n"
    			+ "          ?birthplace2 rdfs:label ?birthplace.\n"
    			+ "          FILTER(lang(?birthplace) = \"en\")}\n"
    			+ "  #optional{?human wdt:P166 ?award.}\n"
    			+ "  optional{?human wdt:P136 ?genre2.\n"
    			+ "          ?genre2 rdfs:label ?genre. \n"
    			+ "          FILTER(lang(?genre) = \"en\")}\n"
    			+ "  optional{?human wdt:P569 ?birth.}\n"
    			+ "  optional{?human wdt:P570 ?death.}\n"
    			+ "      BIND(IF(Bound(?death),YEAR(xsd:dateTime(?death))-YEAR(xsd:dateTime(?birth)),YEAR(NOW())-YEAR(xsd:dateTime(?birth))) AS ?age)\n"
    			+ "  }";
    	String result;
    	result=open(query);
    	send_csv("sample.csv",result);
    }

    public static String open(String query) throws IOException {
        System.out.println("\n -- CONNECT --");
        URL url = new URL("https://query.wikidata.org/sparql?query="+URLEncoder.encode(query , "UTF-8"));
        HttpURLConnection connection = (HttpURLConnection)url.openConnection();
        connection.setRequestMethod("GET");
        connection.setDoInput(true);
        connection.setRequestProperty("Accept", "application/sparql-results+json");
        Scanner s = new Scanner(connection.getInputStream()).useDelimiter("\\A");
        String result = s.hasNext() ? s.next() : "";
        s.close();
        return result;
    }
    
    public static void send_csv(String fileName, String fileText) throws IOException {
        System.out.println("\n -- CREATE FILE --");
        URL url = new URL("http://localhost:50070/webhdfs/v1/user/Werner/" + fileName + "?user.name=hdfs&op=CREATE");
        HttpURLConnection connection = (HttpURLConnection)url.openConnection();
        connection.setRequestMethod("PUT");
        connection.setDoInput(true);
        connection.setDoOutput(true);
        connection.getOutputStream().write(fileText.getBytes());
        Map<String, List<String>> header = connection.getHeaderFields();
        for (String field : header.keySet())
            System.out.println(field + ": " + header.get(field));
    }
    
}

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class Main {

	public static void main(String[] args) throws IOException {
		try {
			String test = open("spotify_ids_chunk.txt");
			// String test="5x2Ufw4gSPVw4TNcGCpFT1, 0tdKRrbItnLj40yUFi23jx";
			String[] list = test.substring(1, test.length() - 1).split(", ");
			for (int i = 0; i < list.length; i++) {
				list[i] = "\"" + list[i] + "\"";
			}

			for (int i = 0; i < list.length; i += 50) {
				TimeUnit.SECONDS.sleep(2);
				String converted = String.join(" ", subArray(list, i, i + 50));
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
				send_json("wikidata_data_chunk.json", result);
			}
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public static <T> T[] subArray(T[] array, int beg, int end) {
		return Arrays.copyOfRange(array, beg, end);
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

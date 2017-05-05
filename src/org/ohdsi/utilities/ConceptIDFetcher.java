package org.ohdsi.utilities;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.unix4j.Unix4j;
import org.unix4j.builder.Unix4jCommandBuilder;
import org.unix4j.unix.grep.GrepOption;

public class ConceptIDFetcher {
	
	public static int fetchConceptID (String query, String filename) throws IOException, InterruptedException {
		Runtime r = Runtime.getRuntime();
		try {
			Process p = r.exec("grep \"" + query + "\" \"\\t" + filename + "\\t\"");
			p.waitFor();
			
			BufferedReader b = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line = b.readLine();
			if (line == null) {
				return -1;
			}
			return Integer.parseInt(line.split("\t")[0]);
		}
		catch (IOException e) {
			String resultStr = Unix4j.cat(filename).grep(GrepOption.ignoreCase, "\t" + query + "\t").toStringResult();
			if (resultStr.equals("")) {
				return -1;
			}
			return Integer.parseInt(resultStr.split("\n")[0].split("\t")[0]);
		}
	}
	
	public static Map<String, Integer> fetchConceptIDs (List<String> queries, String filename) throws IOException, InterruptedException {
		HashMap<String, Integer> result = new HashMap<>();
		Runtime r = Runtime.getRuntime();
		PrintWriter writer = new PrintWriter("temp");
		Iterator<String> iter = queries.iterator();
		while (iter.hasNext()) {
			writer.write(iter.next() + "\n");
			System.out.println("sift --cores=8 -z -i -f temp " + filename);
		}
		writer.flush();
		writer.close();
		Process p = r.exec("sift --cores=8 -z -i -f temp " + "src/org/ohdsi/rabbitInAHat/dataModel/CONCEPT_TRUNCATED.csv.gz");
		//p.waitFor();
		System.out.println("dwndev");
		BufferedReader b = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String line = null;
		while ((line = b.readLine()) != null) {
			System.out.println(line);
			Iterator<String> iter2 = queries.iterator();
			while (iter2.hasNext()) {
				String item = iter2.next();
				if (line.contains(item)) {
					result.put(item, Integer.parseInt(line.split("\t")[0]));
					iter2.remove();
				}
			}
		}
		return result;
	}
	
	public static void main (String[] args) throws IOException, InterruptedException {
		LinkedList<String> l = new LinkedList<>();
		l.add("Dopamine");
		l.add("Alcohol");
		System.out.println(fetchConceptIDs(l, "src/org\\ohdsi/rabbitInAHat/dataModel/CONCEPT_TRUNCATED.csv.gz"));
	}

}


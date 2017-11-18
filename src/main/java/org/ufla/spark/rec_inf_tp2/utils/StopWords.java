package org.ufla.spark.rec_inf_tp2.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class StopWords {

	private static String ARQUIVO_STOP_WORDS = "stoplist.txt";
	private BufferedReader in;

	public String[] getStopWords() throws IOException {
		List<String> stopWords = new ArrayList<>();
		FileReader file = new FileReader(ARQUIVO_STOP_WORDS);
		in = new BufferedReader(file);
		String linha;
		while ((linha = in.readLine()) != null) {
			String[] tokens = linha.split("\\s");
			for (String token : tokens) {
				if (token.isEmpty()) {
					continue;
				}
				stopWords.add(token);
			}
		}
		in.close();
		return stopWords.toArray(new String[0]);
	}

	public Set<String> getSetStopWords() throws IOException {
		return new HashSet<>(Arrays.asList(getStopWords()));
	}
}

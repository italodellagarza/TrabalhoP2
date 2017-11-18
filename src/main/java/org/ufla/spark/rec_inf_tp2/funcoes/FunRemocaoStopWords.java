package org.ufla.spark.rec_inf_tp2.funcoes;
import java.util.Set;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class FunRemocaoStopWords implements MapFunction<Row, Row> {

	private static final long serialVersionUID = 1L;

	/**
	 * Coluna em que deve aplicar o pré-processamento.
	 */
	private int colunaEntrada;
	private int colunaSaida;
	private Set<String> stopWords;

	public FunRemocaoStopWords(int colunaEntrada, int colunaSaida, Set<String> stopWords) {
		this.colunaEntrada = colunaEntrada;
		this.colunaSaida = colunaSaida;
		this.stopWords = stopWords;
	}

	@Override
	public Row call(Row row) throws Exception {
		int n = row.length();
		if (n == colunaSaida) {
			n++;
		}
		Object[] campos = new Object[n];
		for (int i = 0; i < row.length(); i++) {
			campos[i] = row.get(i);
		}
		String[] palavras = row.getString(colunaEntrada).split(" ");
		StringBuilder saida = new StringBuilder();
		for (String palavra : palavras) {
			if (!stopWords.contains(palavra)) {
				saida.append(palavra).append(' ');
			}

		}
		saida.deleteCharAt(saida.length() - 1);
		campos[colunaSaida] = saida.toString();
		return RowFactory.create(campos);
	}

}

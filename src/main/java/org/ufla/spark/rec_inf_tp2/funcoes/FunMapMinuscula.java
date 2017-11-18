package org.ufla.spark.rec_inf_tp2.funcoes;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class FunMapMinuscula implements MapFunction<Row, Row> {

	private static final long serialVersionUID = 1L;

	/**
	 * Coluna em que deve aplicar o pré-processamento.
	 */
	private int colunaEntrada;
	private int colunaSaida;

	public FunMapMinuscula(int colunaEntrada, int colunaSaida) {
		this.colunaEntrada = colunaEntrada;
		this.colunaSaida = colunaSaida;
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
		campos[colunaSaida] = row.getString(colunaEntrada).toLowerCase();
		return RowFactory.create(campos);
	}

}

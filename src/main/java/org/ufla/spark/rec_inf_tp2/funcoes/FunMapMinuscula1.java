package org.ufla.spark.rec_inf_tp2.funcoes;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class FunMapMinuscula1 implements MapFunction<Row, Row> {

	private static final long serialVersionUID = 1L;

	/**
	 * Coluna em que deve aplicar o pré-processamento.
	 */
	private int coluna;

	public FunMapMinuscula1(int coluna) {
		this.coluna = coluna;
	}

	@Override
	public Row call(Row row) throws Exception {
		Object[] campos = new Object[row.length()];
		for (int i = 0; i < row.length(); i++) {
			campos[i] = row.get(i);
		}
		campos[coluna] = row.getString(coluna).toLowerCase();
		return RowFactory.create(campos);
	}

}

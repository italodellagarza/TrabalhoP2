package org.ufla.spark.rec_inf_tp2.funcoes;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import scala.Tuple2;

public class FunParaTuple2 implements MapFunction<Row, Tuple2<Object, Object>> {

	private static final long serialVersionUID = 1L;

	/**
	 * Classe do documento.
	 */
	private int colunaPredicao;
	private int colunaLabel;

	public FunParaTuple2(int colunaPredicao, int colunaLabel) {
		this.colunaPredicao = colunaPredicao;
		this.colunaLabel = colunaLabel;
	}

	/**
	 * Retorna a linha com a inserção da classe do objeto.
	 */
	@Override
	public Tuple2<Object, Object> call(Row row) throws Exception {
		Object predicao = row.get(colunaPredicao);
		Object label = row.get(colunaLabel);
		return new Tuple2<Object, Object>(predicao, label);
	}

}

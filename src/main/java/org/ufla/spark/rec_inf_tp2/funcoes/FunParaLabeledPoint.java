package org.ufla.spark.rec_inf_tp2.funcoes;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.Row;

/**
 * Responsável por converter uma Row em um LabeledPoint.
 * 
 * @author carlos
 * @author douglas
 * @author italo
 */
public class FunParaLabeledPoint implements MapFunction<Row, LabeledPoint> {

	private static final long serialVersionUID = 1L;

	/**
	 * Indice da coluna com o label do documento.
	 */
	private int colunaLabel;
	/**
	 * Indice da coluna com as features do documento.
	 */
	private int colunaFeatures;

	public FunParaLabeledPoint(int colunaLabel, int colunaFeatures) {
		this.colunaLabel = colunaLabel;
		this.colunaFeatures = colunaFeatures;
	}

	/**
	 * Converte a Row no LabeledPoint.
	 */
	@Override
	public LabeledPoint call(Row row) throws Exception {
		Vector vector = new DenseVector(((org.apache.spark.ml.linalg.Vector) row.get(colunaFeatures)).toArray());
		return new LabeledPoint(row.getLong(colunaLabel), vector.compressed());
	}

}

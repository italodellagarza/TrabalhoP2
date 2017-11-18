package org.ufla.spark.rec_inf_tp2.funcoes;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.Row;

public class FunParaLabeledPoint implements MapFunction<Row, LabeledPoint> {

	private static final long serialVersionUID = 1L;

	private int colunaLabel;
	private int colunaFeatures;

	public FunParaLabeledPoint(int colunaLabel, int colunaFeatures) {
		this.colunaLabel = colunaLabel;
		this.colunaFeatures = colunaFeatures;
	}

	@Override
	public LabeledPoint call(Row row) throws Exception {
		Vector vector = new DenseVector(((org.apache.spark.ml.linalg.Vector) row.get(colunaFeatures)).toArray());
		return new LabeledPoint(row.getLong(colunaLabel), vector.compressed());
	}

}

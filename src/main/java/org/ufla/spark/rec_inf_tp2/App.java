package org.ufla.spark.rec_inf_tp2;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.classification.LinearSVC;
import org.apache.spark.ml.classification.OneVsRest;
import org.apache.spark.ml.evaluation.Evaluator;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.ufla.spark.rec_inf_tp2.funcoes.FunParaTuple2;
import org.ufla.spark.rec_inf_tp2.utils.DataSetUtils;

import scala.Tuple2;

/**
 * 
 * @author carlos
 * @author douglas
 * @author italo
 * 
 *         Classe principal da aplicação.
 */
public class App {

	/**
	 * Converte um Dataset<Row> em um Dataset<Tuple2<Object, Object>>.
	 * 
	 * @param dataset
	 *            dataset a ser convertido
	 * @param primeiroElemento
	 *            nome da coluna que será o primeiro elemento da tupla
	 * @param segundoElemento
	 *            nome da coluna que será o segundo elemento da tupla
	 * @return dataset convertido para Tuple2<Object, Object>
	 */
	private static Dataset<Tuple2<Object, Object>> paraTuple2(Dataset<Row> dataset, String primeiroElemento,
			String segundoElemento) {
		@SuppressWarnings("unchecked")
		Class<Tuple2<Object, Object>> tuple2ObjectClasse = (Class<
				Tuple2<Object, Object>>) new Tuple2<Object, Object>(primeiroElemento, segundoElemento).getClass();
		return dataset.map(new FunParaTuple2(DataSetUtils.getIndiceColuna(dataset, primeiroElemento),
				DataSetUtils.getIndiceColuna(dataset, segundoElemento)), Encoders.bean(tuple2ObjectClasse));
	}

	public static void main(String[] args) throws IOException {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		Logger.getLogger("").setLevel(Level.ERROR);

		// Amostragem
		// datasetTreino = datasetTreino.sample(false, 0.01);
		// datasetTeste = datasetTeste.sample(false, 0.01);

		testeModelo00();

	}

	private static void testeModelo00() throws IOException {
		LinearSVC lsvc = new LinearSVC().setMaxIter(10).setRegParam(0.1);
		OneVsRest ovr = new OneVsRest().setClassifier(lsvc);

		ParamMap[] parametrosGrid = new ParamGridBuilder().addGrid(lsvc.regParam(), new double[] { 0.1 }).build();

		testeModelo(TipoBaseDeDados.TREINO_HDFS, TipoBaseDeDados.TESTE_HDFS, PreProcessamento.COMPLETO,
				ExtracaoFeatures.HASHING_TF_IDF, "modeloCV_00_new", parametrosGrid, ovr,
				new MulticlassClassificationEvaluator(), 10);
	}

	@SuppressWarnings("unused")
	private static void testeModelo01() {

	}

	private static void testeModelo(TipoBaseDeDados tipoBaseDeDadosTreino, TipoBaseDeDados tipoBaseDeDadosTeste,
			PreProcessamento preProcessamento, ExtracaoFeatures extracaoDeFeatures, String nomeDoModelo,
			ParamMap[] parametrosGrid, Estimator<?> estimator, Evaluator evaluator, int numFolds) throws IOException {
		BaseDeDados baseDeDados = BaseDeDados.getInstancia();

		Dataset<Row> datasetTreino =
				baseDeDados.leBaseDeDados(tipoBaseDeDadosTreino, preProcessamento, extracaoDeFeatures);
		Dataset<Row> datasetTeste =
				baseDeDados.leBaseDeDados(tipoBaseDeDadosTeste, preProcessamento, extracaoDeFeatures);

		CrossValidatorModel modeloCV;
		String predicoesNome = "predicoes_" + nomeDoModelo;
		File diretorioModelo = new File(nomeDoModelo);
		File diretorioPredicoes = new File(predicoesNome);
		if (diretorioPredicoes.exists() && diretorioModelo.exists()) {
			modeloCV = CrossValidatorModel.read().load(nomeDoModelo);
		} else {
			if (diretorioModelo.exists()) {
				diretorioModelo.delete();
			}
			if (diretorioPredicoes.exists()) {
				diretorioPredicoes.delete();
			}
			CrossValidator cv = new CrossValidator().setEstimator(estimator).setEvaluator(evaluator)
					.setEstimatorParamMaps(parametrosGrid).setNumFolds(numFolds);

			modeloCV = cv.fit(datasetTreino);
			modeloCV.save(nomeDoModelo);
		}

		Dataset<Row> predicoes;

		if (new File(predicoesNome).exists()) {
			predicoes = Configuracao.getInstancia().getSessaoSpark().read().load(predicoesNome);
		} else {
			System.out.println(datasetTeste.schema());
			predicoes = modeloCV.bestModel().transform(datasetTeste);
			predicoes.write().save(predicoesNome);
		}

		Dataset<Tuple2<Object, Object>> predicoesTuple2 = paraTuple2(predicoes, "prediction", "label");

		// Get evaluation metrics.
		MulticlassMetrics metrics = new MulticlassMetrics(predicoesTuple2.rdd());

		// Confusion matrix
		Matrix confusion = metrics.confusionMatrix();
		System.out.println("\nConfusion matrix: \n" + confusion);


		// Stats by labels
		for (int i = 0; i < metrics.labels().length; i++) {
			System.out.format("\n\nClass %f precision = %f\n", metrics.labels()[i], metrics.precision(metrics.labels()[i]));
			System.out.format("Class %f recall = %f\n", metrics.labels()[i], metrics.recall(metrics.labels()[i]));
			System.out.format("Class %f F1 score = %f\n", metrics.labels()[i], metrics.fMeasure(metrics.labels()[i]));
		}

		// Weighted stats
		System.out.println("\n\nMétricas calculadas com org.apache.spark.mllib.evaluation.MulticlassMetrics\n");
		System.out.format("Accuracy = %f\n", metrics.accuracy());
		System.out.format("Weighted precision = %f\n", metrics.weightedPrecision());
		System.out.format("Weighted recall = %f\n", metrics.weightedRecall());
		System.out.format("Weighted F1 score = %f\n", metrics.weightedFMeasure());
		System.out.format("Weighted false positive rate = %f\n", metrics.weightedFalsePositiveRate());
		System.out.format("Weighted true positive rate = %f\n", metrics.weightedTruePositiveRate());

		
		System.out.println("\n\nMétricas calculadas org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator\n");
		MulticlassClassificationEvaluator mEvaluator =
				new MulticlassClassificationEvaluator();
		System.out.println("F1 -> " + mEvaluator.evaluate(predicoes));
		mEvaluator.setMetricName("accuracy");
		System.out.println("Accuracy -> " + mEvaluator.evaluate(predicoes));
		mEvaluator.setMetricName("weightedPrecision");
		System.out.println("weightedPrecision -> " + mEvaluator.evaluate(predicoes));
		mEvaluator.setMetricName("weightedRecall");
		System.out.println("weightedRecall -> " + mEvaluator.evaluate(predicoes));
		
		System.out.println(
				"\nmedia das métricas F1 para cross validator em treino -> " + Arrays.toString(modeloCV.avgMetrics()));

	}

}

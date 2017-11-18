package org.ufla.spark.rec_inf_tp2;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LinearSVC;
import org.apache.spark.ml.classification.OneVsRest;
import org.apache.spark.ml.evaluation.Evaluator;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.ufla.spark.rec_inf_tp2.funcoes.FunParaTuple2;
import org.ufla.spark.rec_inf_tp2.utils.DataSetUtils;

import scala.Tuple2;

import static org.ufla.spark.rec_inf_tp2.BaseDeDados.*;

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
	@SuppressWarnings("unused")
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

		testeModelo(TipoBaseDeDados.TREINO_JSON_HDFS, TipoBaseDeDados.TESTE_JSON_HDFS, PreProcessamento.COMPLETO,
				ExtracaoDeFeatures.HASHING_TF_IDF, "modeloCV_00", parametrosGrid, ovr,
				new MulticlassClassificationEvaluator(), 3);
	}

	private static void testeModelo01() {

	}

	private static void testeModelo(TipoBaseDeDados tipoBaseDeDadosTreino, TipoBaseDeDados tipoBaseDeDadosTeste,
			PreProcessamento preProcessamento, ExtracaoDeFeatures extracaoDeFeatures, String nomeDoModelo,
			ParamMap[] parametrosGrid, Estimator estimator, Evaluator evaluator, int numFolds) throws IOException {
		BaseDeDados baseDeDados = BaseDeDados.getInstancia();

		Dataset<Row> datasetTreino =
				baseDeDados.leBaseDeDados(tipoBaseDeDadosTreino, preProcessamento, extracaoDeFeatures);
		Dataset<Row> datasetTeste =
				baseDeDados.leBaseDeDados(tipoBaseDeDadosTeste, preProcessamento, extracaoDeFeatures);

		CrossValidatorModel modeloCV;

		if (new File(nomeDoModelo).exists()) {
			modeloCV = CrossValidatorModel.read().load(nomeDoModelo);
		} else {
			LinearSVC lsvc = new LinearSVC().setMaxIter(10).setRegParam(0.1);
			OneVsRest ovr = new OneVsRest().setClassifier(lsvc);

			CrossValidator cv = new CrossValidator().setEstimator(ovr).setEvaluator(evaluator)
					.setEstimatorParamMaps(parametrosGrid).setNumFolds(numFolds);

			modeloCV = cv.fit(datasetTreino);
			modeloCV.save(nomeDoModelo);
		}

		System.out.println(
				"\nmedia das métricas F1 para cross validator em treino -> " + Arrays.toString(modeloCV.avgMetrics()));
		// Model<?> bestModel = modeloCV.bestModel();

		Dataset<Row> predicoes = modeloCV.transform(datasetTeste);
		System.out.println(
				"\nmedia das métricas F1 para modelo em teste -> " + evaluator.evaluate(predicoes));
	}

}

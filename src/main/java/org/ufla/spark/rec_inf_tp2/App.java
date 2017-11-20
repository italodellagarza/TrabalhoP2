package org.ufla.spark.rec_inf_tp2;

import static org.ufla.spark.rec_inf_tp2.BaseDeDados.CARACTERISTICAS_TF_COL;
import static org.ufla.spark.rec_inf_tp2.BaseDeDados.CONTEUDO_FINAL_PRE_PROC_COL;
import static org.ufla.spark.rec_inf_tp2.BaseDeDados.PALAVRAS_COL;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LinearSVC;
import org.apache.spark.ml.classification.OneVsRest;
import org.apache.spark.ml.evaluation.Evaluator;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
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
import org.ufla.spark.rec_inf_tp2.utils.DatasetUtils;

import scala.Tuple2;

/**
 * Classe principal da aplicação.
 * 
 * @author carlos
 * @author douglas
 * @author italo
 */
public class App {

	/**
	 * setNumFeatures(int value) The default feature dimension is 2^(18) = 262144.
	 * VALUES -> { 16384, 262144, 4194304 }.
	 */
	private static int[] numFeatures = new int[] { 16384, 262144, 4194304 };
	/**
	 * setAggregationDepth(int value) Suggested depth for treeAggregate (greater
	 * than or equal to 2). If the dimensions of features or the number of
	 * partitions are large, this param could be adjusted to a larger size. Default
	 * is 2. VALUES -> { 2, 256, 1024 }
	 */
	private static int[] aggregationDepths = new int[] { 2, 256, 1024 };
	/**
	 * setMaxIter(int value) Set the maximum number of iterations. Default is 100.
	 * VALUES -> { 10, 100, 1000 }
	 */
	private static int[] maxIters = new int[] { 0, 100, 1000 };
	/**
	 * setRegParam(double value) Set the regularization parameter. Default is 0.0.
	 * VALUES -> { 0.0, 0.00001, 0.001 }
	 */
	private static double[] regParams = new double[] { 0.0, 0.00001, 0.001 };
	/**
	 * setTol(double value) Set the convergence tolerance of iterations. Smaller
	 * values will lead to higher accuracy at the cost of more iterations. Default
	 * is 1E-6. VALUES -> { 1E-6, 1E-9, 1E-12 }
	 */
	private static double[] tols = new double[] { 1E-6, 1E-9, 1E-12 };
	/**
	 * Número de conjuntos em que a base de dados de treino será distribuida para
	 * realizar a validação cruzada.
	 */
	private static int numFolds = 10;
	/**
	 * Nome do diretório que irá salvar o melhor modelo encontrado pela busca em
	 * grid.
	 */
	private static String modeloGridSearch = "modeloFinalGridSearch";

	public static void main(String[] args) throws IOException {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		Logger.getLogger("").setLevel(Level.ERROR);
	}

	/**
	 * Teste para um modelo de SVM.
	 * 
	 * @throws IOException
	 */
	private static void testeModelo00() throws IOException {
		LinearSVC lsvc = new LinearSVC().setMaxIter(10).setRegParam(0.1);
		OneVsRest ovr = new OneVsRest().setClassifier(lsvc);

		ParamMap[] parametrosGrid = new ParamGridBuilder().addGrid(lsvc.regParam(), new double[] { 0.1 }).build();

		testeModelo(TipoBaseDeDados.TREINO_HDFS, TipoBaseDeDados.TESTE_HDFS, PreProcessamento.COMPLETO,
				ExtracaoFeatures.HASHING_TF_IDF, "modeloCV_00", parametrosGrid, ovr,
				new MulticlassClassificationEvaluator(), 10);
	}

	/**
	 * Realiza busca no grid implementada pela classe CombinacaoParametros
	 * 
	 * @throws IOException
	 */
	@SuppressWarnings("unused")
	private static void testeModeloCombinandoParametros() throws IOException {
		File diretorioModeloFinal = new File(modeloGridSearch);
		File diretorioPredicoesModFin = new File("predicoes_" + modeloGridSearch + ".txt");
		File arquivoLog = new File("log_" + modeloGridSearch);
		if (arquivoLog.exists()) {
			arquivoLog.delete();
		}
		arquivoLog.createNewFile();
		BufferedWriter bwLog = new BufferedWriter(new FileWriter(arquivoLog));

		CombinacaoParametros combinacaoParametros =
				new CombinacaoParametros(aggregationDepths, maxIters, regParams, tols);

		BaseDeDados baseDeDados = BaseDeDados.getInstancia();
		Dataset<Row> datasetTreinoPreProc = baseDeDados.leBaseDeDados(TipoBaseDeDados.TREINO_HDFS,
				PreProcessamento.COMPLETO, ExtracaoFeatures.SEM_SELECAO);
		Dataset<Row> datasetTesteoPreProc = baseDeDados.leBaseDeDados(TipoBaseDeDados.TESTE_HDFS,
				PreProcessamento.COMPLETO, ExtracaoFeatures.SEM_SELECAO);
		int contModels = 0;

		double melhorF1 = Double.MIN_VALUE;
		Parametros melhoresParametros = null;
		int melhorNumFeatures = -1;

		for (int nFeatures : numFeatures) {
			ExtracaoFeatures.HASHING_TF_IDF.setQtdFeatures(nFeatures);
			Dataset<Row> datasetTreinoFeatures =
					baseDeDados.aplicarExtracaoDeFeatures(datasetTreinoPreProc, ExtracaoFeatures.HASHING_TF_IDF);
			Dataset<Row> datasetTesteFeatures =
					baseDeDados.aplicarExtracaoDeFeatures(datasetTesteoPreProc, ExtracaoFeatures.HASHING_TF_IDF);
			while (combinacaoParametros.temProximo()) {
				Parametros parametros = combinacaoParametros.proximo();
				if (contModels > -1) {
					LinearSVC lsvc = new LinearSVC().setAggregationDepth(parametros.aggregationDepth)
							.setMaxIter(parametros.maxIter).setRegParam(parametros.regParam).setTol(parametros.tol);
					OneVsRest ovr = new OneVsRest().setClassifier(lsvc);
					CrossValidator cv = new CrossValidator().setEstimator(ovr)
							.setEvaluator(new MulticlassClassificationEvaluator()).setNumFolds(numFolds);
					CrossValidatorModel modeloCV = cv.fit(datasetTreinoFeatures);
					double f1 = modeloCV.avgMetrics()[0];
					if (melhorF1 < f1) {
						System.out.println("\nATUALIZAÇÃO DO MODELO! NÃO FECHAR APLICAÇÃO!\n");
						melhorF1 = f1;
						melhoresParametros = parametros;
						melhorNumFeatures = nFeatures;
						bwLog.write(String.format("Atualização de melhor F1!\n"));
						bwLog.write(String.format("Modelo %d com F1 = %f\n", contModels, f1));
						bwLog.write(String.format("numFeatures: %d\n", nFeatures));
						bwLog.write(String.format("aggregationDepth: %d\n", parametros.aggregationDepth));
						bwLog.write(String.format("maxIter: %d\n", parametros.maxIter));
						bwLog.write(String.format("regParam: %f\n", parametros.regParam));
						bwLog.write(String.format("tol %f\n", parametros.tol));
						bwLog.write(String.format("---------------------------------------------------\n"));
						bwLog.flush();
						if (diretorioModeloFinal.exists()) {
							diretorioModeloFinal.delete();
						}
						modeloCV.write().save(diretorioModeloFinal.getAbsolutePath());

						Dataset<Row> predicoes = modeloCV.bestModel().transform(datasetTesteFeatures);
						if (diretorioPredicoesModFin.exists()) {
							diretorioPredicoesModFin.delete();
						}
						predicoes.write().save(diretorioPredicoesModFin.getAbsolutePath());
						System.out.println("\nMODELO ATUALIZADO!\n");
					}
				}
				contModels++;
			}
		}
		bwLog.close();
	}

	/**
	 * Realiza a busca no grid implementada pelo próprio Spark.
	 * 
	 * @throws IOException
	 */
	@SuppressWarnings("unused")
	private static void testeModeloGridSearch() throws IOException {
		File diretorioModeloFinal = new File(modeloGridSearch);
		File diretorioPredicoesModFin = new File("predicoes_" + modeloGridSearch + ".txt");
		BaseDeDados baseDeDados = BaseDeDados.getInstancia();
		Dataset<Row> datasetTreinoPreProc = baseDeDados.leBaseDeDados(TipoBaseDeDados.TREINO_HDFS,
				PreProcessamento.COMPLETO, ExtracaoFeatures.SEM_SELECAO);
		Dataset<Row> datasetTestePreProc = baseDeDados.leBaseDeDados(TipoBaseDeDados.TESTE_HDFS,
				PreProcessamento.COMPLETO, ExtracaoFeatures.SEM_SELECAO);

		List<PipelineStage> transformacoes = ExtracaoFeatures.HASHING_TF_IDF.getTransformacoes();
		LinearSVC lsvc = new LinearSVC();
		OneVsRest ovr = new OneVsRest().setClassifier(lsvc);
		transformacoes.add(ovr);
		HashingTF hashingTF = (HashingTF) transformacoes.get(1);
		Pipeline pipeline = new Pipeline().setStages(transformacoes.toArray(new PipelineStage[0]));

		MulticlassClassificationEvaluator mEvaluator = new MulticlassClassificationEvaluator();

		ParamMap[] parametrosGrid = new ParamGridBuilder().addGrid(hashingTF.numFeatures(), numFeatures)
				.addGrid(lsvc.maxIter(), maxIters).addGrid(lsvc.aggregationDepth(), aggregationDepths)
				.addGrid(lsvc.regParam(), regParams).addGrid(lsvc.tol(), tols).build();

		CrossValidator cv = new CrossValidator().setEstimator(pipeline).setEstimatorParamMaps(parametrosGrid)
				.setEvaluator(mEvaluator).setNumFolds(numFolds);

		CrossValidatorModel modeloCV = cv.fit(datasetTreinoPreProc);

		modeloCV.write().save(diretorioModeloFinal.getAbsolutePath());

		int nFeatures = (int) modeloCV.bestModel().getOrDefault(hashingTF.numFeatures());
		ExtracaoFeatures.HASHING_TF_IDF.setQtdFeatures(nFeatures);
		Dataset<Row> datasetTeste =
				baseDeDados.aplicarExtracaoDeFeatures(datasetTestePreProc, ExtracaoFeatures.HASHING_TF_IDF);

		Dataset<Row> predicoes = modeloCV.bestModel().transform(datasetTeste);
		predicoes.write().save(diretorioPredicoesModFin.getAbsolutePath());

	}

	/**
	 * Teste para verificar o VocabSize default de CountVectorizer.
	 */
	@SuppressWarnings("unused")
	private static void testeCountVectorizer() {
		BaseDeDados baseDeDados = BaseDeDados.getInstancia();

		Dataset<Row> datasetTreino = baseDeDados.leBaseDeDados(TipoBaseDeDados.TREINO_HDFS, PreProcessamento.COMPLETO,
				ExtracaoFeatures.SEM_SELECAO);
		Tokenizer tokenizer = new Tokenizer().setInputCol(CONTEUDO_FINAL_PRE_PROC_COL).setOutputCol(PALAVRAS_COL);

		CountVectorizer countVectorizer =
				new CountVectorizer().setInputCol(tokenizer.getOutputCol()).setOutputCol(CARACTERISTICAS_TF_COL);

		datasetTreino = tokenizer.transform(datasetTreino);

		CountVectorizerModel coVectorizerModel = countVectorizer.fit(datasetTreino);

		System.out.println("\n CountVectorizer dados:");
		System.out.println("VocabSize -> " + countVectorizer.getVocabSize());
		System.out.println("MinTF -> " + countVectorizer.getMinTF());
		System.out.println("MinDF -> " + countVectorizer.getMinDF() + "\n");
		System.out.println("--------------------------------------------\n");
		System.out.println("\n CountVectorizerModel dados:");
		System.out.println("VocabSize -> " + coVectorizerModel.getVocabSize());
		System.out.println("MinTF -> " + coVectorizerModel.getMinTF());
		System.out.println("MinDF -> " + coVectorizerModel.getMinDF() + "\n");

		System.exit(0);
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
			modeloCV.write().save(nomeDoModelo);
		}

		Dataset<Row> predicoes;

		if (new File(predicoesNome).exists()) {
			predicoes = Configuracao.getInstancia().getSessaoSpark().read().load(predicoesNome);
		} else {
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
			System.out.format("\n\nClass %f precision = %f\n", metrics.labels()[i],
					metrics.precision(metrics.labels()[i]));
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

		System.out
				.println("\n\nMétricas calculadas org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator\n");
		MulticlassClassificationEvaluator mEvaluator = new MulticlassClassificationEvaluator();
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
		return dataset.map(new FunParaTuple2(DatasetUtils.getIndiceColuna(dataset, primeiroElemento),
				DatasetUtils.getIndiceColuna(dataset, segundoElemento)), Encoders.bean(tuple2ObjectClasse));
	}

}

/**
 * Parêmetros utilizados para o grid search.
 * 
 * @author carlos
 * @author douglas
 * @author italo
 */
class Parametros {

	int aggregationDepth;
	int maxIter;
	double regParam;
	double tol;

	public Parametros(int aggregationDepth, int maxIter, double regParam, double tol) {
		this.aggregationDepth = aggregationDepth;
		this.maxIter = maxIter;
		this.regParam = regParam;
		this.tol = tol;
	}

}

/**
 * Responsável por realizar as combinações de parâmetros, a busca no grid.
 * 
 * @author carlos
 * @author douglas
 * @author italo
 */
class CombinacaoParametros {

	private int[] aggregationDepths;
	private int[] maxIters;
	private double[] regParams;
	private double[] tols;

	private int iAggregationDepth = 0;
	private int iMaxIter = 0;
	private int iRegParam = 0;
	private int iTol = 0;

	public CombinacaoParametros(int[] aggregationDepths, int[] maxIters, double[] regParams, double[] tols) {
		this.aggregationDepths = aggregationDepths;
		this.maxIters = maxIters;
		this.regParams = regParams;
		this.tols = tols;
	}

	/**
	 * Verifica se tem uma próxima combinação de parâmetros.
	 * 
	 * @return true se tem uma próxima combinação de parâmetros, caso contrário,
	 *         false.
	 */
	public boolean temProximo() {
		return iAggregationDepth < aggregationDepths.length;
	}

	/**
	 * Retorna a próxima combinação de parâmetros da busca em no grid, ou null se
	 * todas as combinações já foram retornadas.
	 * 
	 * @return próxima combinação de parâmetros da busca em no grid, ou null se
	 *         todas as combinações já foram retornadas.
	 */
	public Parametros proximo() {
		if (iAggregationDepth == aggregationDepths.length) {
			return null;
		}
		Parametros parametros = new Parametros(aggregationDepths[iAggregationDepth], maxIters[iMaxIter],
				regParams[iRegParam], tols[iTol]);
		atualizarIndices();
		return parametros;
	}

	/**
	 * Atualiza os índices para a próxima combinação de parâmetros.
	 */
	private void atualizarIndices() {
		iTol++;
		if (iTol == tols.length) {
			iTol = 0;
			iRegParam++;
		}
		if (iRegParam == regParams.length) {
			iRegParam = 0;
			iMaxIter++;
		}
		if (iMaxIter == maxIters.length) {
			iMaxIter = 0;
			iAggregationDepth++;
		}
	}

}

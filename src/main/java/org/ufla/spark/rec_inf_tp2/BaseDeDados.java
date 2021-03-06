package org.ufla.spark.rec_inf_tp2;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.ufla.spark.rec_inf_tp2.funcoes.FunInsereClasse;
import org.ufla.spark.rec_inf_tp2.utils.DatasetUtils;

/**
 * Responsável pelas operações de leitura da base de dados. Padrão singleton.
 * 
 * @author carlos
 * @author douglas
 * @author italo
 */
public class BaseDeDados {

	/**
	 * Única instância de base de dados, padrão singleton.
	 */
	private static BaseDeDados baseDeDados;

	/**
	 * Representa os nomes das colunas do esquema original
	 */
	private static final String[] ESQUEMA_STRING = { "nome", "conteudo", "label_str", "label" };

	public static final String NOME_COL = "nome";
	public static final String CONTEUDO_COL = "conteudo";
	public static final String LABEL_COL = "label";
	public static final String LABEL_STR_COL = "label_str";
	public static final String CONTEUDO_MINUSCULO_COL = "conteudo_minusculo";
	public static final String CONTEUDO_MINUSCULO_TAGS_COL = "conteudo_minusculo_tags";
	public static final String CONTEUDO_MINUSCULO_TAGS_ASCII_COL = "conteudo_minusculo_tags_ascii";
	public static final String CONTEUDO_SEM_STOP_WORDS_COL = "conteudo_sem_stop_words";
	public static final String CONTEUDO_FINAL_PRE_PROC_COL = "conteudo_final_pre_proc";
	public static final String PALAVRAS_COL = "palavras";
	public static final String CARACTERISTICAS_TF_COL = "caracteristicas_TF";
	public static final String FEATURES_COL = "features";
	/**
	 * Localização relativa (em relação a localização da base de dados Reuters
	 * 21578) do documento utilizado como exemplo para demonstrar o
	 * pré-processamento utilizado.
	 */
	private static final String DOCUMENTO_EXEMPLO_PRE_PROCESSAMENTO =
			File.separator + "training" + File.separator + "acq" + File.separator + "0000005";

	/**
	 * Retorna a única instância de BaseDeDados da aplicação.
	 * 
	 * @return única instância de BaseDeDados da aplicação.
	 */
	public static BaseDeDados getInstancia() {
		if (baseDeDados == null) {
			baseDeDados = new BaseDeDados();
		}
		return baseDeDados;
	}
	/**
	 * Mapeamento de uma classe para seu código.
	 */
	private Map<String, Integer> classesToCodigo = new HashMap<>();

	/**
	 * Contador de classes da base de dados.
	 */
	private int contClasses = 0;

	/**
	 * Construtor da base de dados.
	 * 
	 */
	private BaseDeDados() {

	}

	/**
	 * Aplica uma determinada extração de features em uma determinada base de dados.
	 * 
	 * @param dataset
	 *            base de dados em que a extração de features será realizada
	 * @param extracaoFeatures
	 *            extração de features a ser realizada
	 * @return base de dados após aplica a extração de features
	 */
	public Dataset<Row> aplicarExtracaoDeFeatures(Dataset<Row> dataset, ExtracaoFeatures extracaoFeatures) {
		List<PipelineStage> stages = extracaoFeatures.getTransformacoes();
		System.out.println("\nNUM_FEATURES -> " + ((HashingTF) stages.get(1)).getNumFeatures() + "\n");
		Pipeline pipeline = new Pipeline().setStages(stages.toArray(new PipelineStage[0]));
		return pipeline.fit(dataset).transform(dataset).select(NOME_COL, FEATURES_COL, LABEL_COL, LABEL_STR_COL);
	}

	/**
	 * Aplica um determinado pré-processamento em um determinado dataset.
	 * 
	 * @param dataset
	 *            dataset a ser aplicado o pré-processamento
	 * @param preProcessamento
	 *            pré-processamento a ser aplicado no dataset
	 * @return dataset após a aplicação do pré-processamento
	 */
	public Dataset<Row> aplicarPreProcessamento(Dataset<Row> dataset, PreProcessamento preProcessamento) {
		Transformer[] preProcessamentos = preProcessamento.getTransformacoes();
		Pipeline pipeline = new Pipeline().setStages(preProcessamentos);
		return pipeline.fit(dataset).transform(dataset);
	}

	/**
	 * Cria o esquema original da base de dados.
	 * 
	 * @return esquema original da base de dados
	 */
	public StructType criarEsquemaOriginal() {
		StructField[] campos = new StructField[ESQUEMA_STRING.length];
		for (int i = 0; i < ESQUEMA_STRING.length - 1; i++) {
			campos[i] = DataTypes.createStructField(ESQUEMA_STRING[i], DataTypes.StringType, true);
		}
		campos[ESQUEMA_STRING.length - 1] =
				DataTypes.createStructField(ESQUEMA_STRING[ESQUEMA_STRING.length - 1], DataTypes.IntegerType, true);
		return DataTypes.createStructType(campos);
	}

	/**
	 * Demonstra um determinado pré-processamento no documento de exemplo.
	 * 
	 * @param preProcessamento
	 *            pré-processamento a ser demonstrado
	 */
	public void demonstrarPreProcessamento(PreProcessamento preProcessamento) {
		Transformer[] transformers = preProcessamento.getTransformacoes();
		Dataset<Row> dataset =
				leBaseDeDados(TipoBaseDeDados.TREINO_HDFS, PreProcessamento.NENHUM, ExtracaoFeatures.SEM_SELECAO);
		String arquivoNome = "file:" + Configuracao.getInstancia().getReutersBD().getAbsolutePath()
				+ DOCUMENTO_EXEMPLO_PRE_PROCESSAMENTO;
		dataset.createOrReplaceTempView("dataset_treino");
		dataset = dataset.sparkSession()
				.sql("SELECT * FROM dataset_treino WHERE " + NOME_COL + " = '" + arquivoNome + "'");
		for (int i = 0; i < transformers.length; i++) {
			dataset = transformers[i].transform(dataset);
		}
		DatasetUtils.printRow(dataset.first(), dataset.schema());
	}

	/**
	 * Realiza a leitura de um determinado tipo de base de dados com um determinado
	 * tipo de pré-processamento.
	 * 
	 * @param tipoBaseDeDados
	 *            tipo da base de dados a ser lida
	 * @param preProcessamento
	 *            tipo de pré-processamento aplicado na base de dados
	 * @return base de dados do tipo especificado com o pré-processamento
	 *         selecionado
	 */
	public Dataset<Row> leBaseDeDados(TipoBaseDeDados tipoBaseDeDados, PreProcessamento preProcessamento,
			ExtracaoFeatures extracaoDeFeatures) {
		if (tipoBaseDeDados.eOriginal() && (!PreProcessamento.NENHUM.equals(preProcessamento)
				|| !ExtracaoFeatures.SEM_SELECAO.equals(extracaoDeFeatures))) {
			tipoBaseDeDados = tipoBaseDeDados.paraHDFS();
		}
		Configuracao configuracao = Configuracao.getInstancia();
		File diretorio = configuracao.diretorioBD(tipoBaseDeDados, preProcessamento, extracaoDeFeatures);
		if (diretorio.exists()) {
			System.out.printf(
					"\nLendo base de dados %s com o pré-processamento %s e extração de features %s do diretório %s.\n",
					tipoBaseDeDados, preProcessamento, extracaoDeFeatures, diretorio.getAbsolutePath());
			if (tipoBaseDeDados.eOriginal()) {
				return lerBaseDeDadosOriginal(diretorio);
			} else {
				return configuracao.getSessaoSpark().read().load(diretorio.getAbsolutePath());
			}
		}
		System.out.printf("\nBase de dados %s com o pré-processamento %s não está salva no diretório %s.\n",
				tipoBaseDeDados, preProcessamento, diretorio.getAbsolutePath());
		if (!ExtracaoFeatures.SEM_SELECAO.equals(extracaoDeFeatures)) {
			Dataset<Row> dataset = leBaseDeDados(tipoBaseDeDados, preProcessamento, ExtracaoFeatures.SEM_SELECAO);
			System.out.printf("Aplicando a extração de feature %s na base de dados %s\n", extracaoDeFeatures,
					tipoBaseDeDados);
			return aplicarExtracaoDeFeatures(dataset, extracaoDeFeatures);
		}
		if (!PreProcessamento.NENHUM.equals(preProcessamento)) {
			File diretorioSemFeatures =
					configuracao.diretorioBD(tipoBaseDeDados, preProcessamento, ExtracaoFeatures.SEM_SELECAO);
			Dataset<Row> dataset =
					leBaseDeDados(tipoBaseDeDados, PreProcessamento.NENHUM, ExtracaoFeatures.SEM_SELECAO);
			System.out.printf("Aplicando o pré-processamento %s na base de dados %s\n", preProcessamento,
					tipoBaseDeDados);
			dataset = aplicarPreProcessamento(dataset, preProcessamento);
			System.out.printf("Salvando base de dados %s com o pré-processamento %s no diretório %s.\n",
					tipoBaseDeDados, preProcessamento, diretorioSemFeatures.getAbsolutePath());
			dataset = dataset.select(NOME_COL, CONTEUDO_FINAL_PRE_PROC_COL, LABEL_COL, LABEL_STR_COL);
			dataset.write().save(diretorioSemFeatures.getAbsolutePath());
			return dataset;
		}
		if (!tipoBaseDeDados.eOriginal()) {
			TipoBaseDeDados baseDeDadosOriginal = tipoBaseDeDados.paraOriginal();
			File diretorioOriginal =
					configuracao.diretorioBD(baseDeDadosOriginal, preProcessamento, ExtracaoFeatures.SEM_SELECAO);
			System.out.printf("\nLendo base de dados %s com o pré-processamento %s do diretório %s.\n",
					baseDeDadosOriginal, preProcessamento, diretorioOriginal.getAbsolutePath());
			Dataset<Row> datasetOriginal =
					leBaseDeDados(baseDeDadosOriginal, preProcessamento, ExtracaoFeatures.SEM_SELECAO);
			System.out.printf("Salvando base de dados %s com o pré-processamento %s no diretório %s.\n",
					tipoBaseDeDados, preProcessamento, diretorioOriginal.getAbsolutePath());
			datasetOriginal.write().save(diretorio.getAbsolutePath());
			return datasetOriginal;
		}
		System.out.printf("\nNão foi possível ler a base de dados %s com o pré-processamento %s no diretório %s.\n",
				tipoBaseDeDados, preProcessamento, diretorio.getAbsolutePath());
		return null;
	}

	/**
	 * Realiza a leitura da base de dados original.
	 * 
	 * @param diretorio
	 *            diretório da base de dados original (treino ou teste).
	 * @return base de dados com esquema original
	 */
	private Dataset<Row> lerBaseDeDadosOriginal(File diretorio) {
		Configuracao configuracao = Configuracao.getInstancia();
		StructType esquema = criarEsquemaOriginal();
		Dataset<Row> dataset = configuracao.getSessaoSpark().createDataFrame(new ArrayList<Row>(), esquema);
		for (File diretorioTema : diretorio.listFiles()) {
			String classe = diretorioTema.getName();
			int codigoClasse = classesToCodigo.getOrDefault(classe, -1);
			if (codigoClasse == -1) {
				codigoClasse = contClasses;
				classesToCodigo.put(classe, contClasses);
				contClasses++;
			}
			if (!diretorioTema.isDirectory()) {
				continue;
			}
			JavaPairRDD<String, String> parRDD =
					configuracao.getContextoSpark().wholeTextFiles(diretorioTema.getAbsolutePath());
			JavaRDD<Row> rdd = parRDD.map(new FunInsereClasse(classe, codigoClasse));
			Dataset<Row> temaDataset = configuracao.getSessaoSpark().createDataFrame(rdd, esquema);
			dataset = dataset.union(temaDataset);
		}
		return dataset;
	}

}

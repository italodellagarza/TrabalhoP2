package org.ufla.spark.rec_inf_tp2;

import java.io.File;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * Responsável por manter as configurações da base de dados Reuters 21578 e da
 * aplicação Spark. Utiliza padrão Singleton. *
 * 
 * @author carlos
 * @author douglas
 * @author italo
 */
public class Configuracao {

	/**
	 * Endereço padrão da base de dados Reuters 21578.
	 */
	private static final String REUTERS_BD = "Reuters21578-Apte-90Cat";
	/**
	 * O nome padrão da aplicação Spark.
	 */
	private static final String NOME_APP = "AnaliseSVM";
	/**
	 * Única instância de configuração, padrão singleton.
	 */
	private static Configuracao configuracao;
	/**
	 * Retorna a única instância de Configuração da aplicação.
	 * 
	 * @return única instância de Configuração da aplicação.
	 */
	public static Configuracao getInstancia() {
		if (configuracao == null) {
			configuracao = new Configuracao(REUTERS_BD);
		}
		return configuracao;
	}

	/**
	 * Diretório raiz da base de dados Reuters 21578.
	 */
	private File reutersBD;
	/**
	 * Configuração da aplicação Spark.
	 */
	private SparkConf configuracaoSpark;
	/**
	 * Contexto da aplicação Spark.
	 */
	private JavaSparkContext contextoSpark;

	/**
	 * Sessão da aplicação Spark.
	 */
	private SparkSession sessaoSpark;

	/**
	 * Construtor de configuração com o uso da localização da base de dados Reuters
	 * 21578.
	 * 
	 * @param reutersBD
	 *            localização da base de dados Reuters 21578.
	 */
	private Configuracao(String reutersBD) {
		this.configuracaoSpark = new SparkConf().setMaster("local[*]").setAppName(NOME_APP);
		this.contextoSpark = new JavaSparkContext(configuracaoSpark);
		this.sessaoSpark = SparkSession.builder().config(configuracaoSpark).getOrCreate();
		this.reutersBD = new File(reutersBD);
	}

	/**
	 * Recupera o diretório de um determinado tipo da base de dados Reuters 21578
	 * com um determinado tipo de pré-processamento.
	 * 
	 * @param tipoBaseDeDados
	 *            tipo da base de dados a ser recuperado o diretório
	 * @param preProcessamento
	 *            tipo de pré-processamemto que foi aplicado na base de dados
	 * @param extracaoDeFeatures
	 *            tipo de extração de features que foi aplicado na base de dados
	 * @return diretório da base de dados especificada
	 */
	public File diretorioBD(TipoBaseDeDados tipoBaseDeDados, PreProcessamento preProcessamento,
			ExtracaoFeatures extracaoDeFeatures) {
		return new File(reutersBD.getAbsolutePath() + File.separator + tipoBaseDeDados.getNomeDiretorioBD()
				+ preProcessamento.getSufixoDiretorioBD() + extracaoDeFeatures.getSufixoDiretorioBD());
	}

	/**
	 * Recupera a configuração da aplicação Spark.
	 * 
	 * @return configuração da aplicação Spark
	 */
	public SparkConf getConfiguracaoSpark() {
		return configuracaoSpark;
	}

	/**
	 * Recupera o contexto da aplicação Spark.
	 * 
	 * @return contexto da aplicação Spark
	 */
	public JavaSparkContext getContextoSpark() {
		return contextoSpark;
	}

	/**
	 * Recupera o diretório raiz da base de dados Reuters 21578.
	 * 
	 * @return diretório raiz da base de dados Reuters 21578
	 */
	public File getReutersBD() {
		return reutersBD;
	}

	/**
	 * Recupera a sessão da aplicação Spark.
	 * 
	 * @return sessão da aplicação Spark
	 */
	public SparkSession getSessaoSpark() {
		return sessaoSpark;
	}

	/**
	 * Define o endereço da base de dados Reuters 21578.
	 * 
	 * @param reutersBD
	 *            endereço da base de dados Reuters 21578
	 */
	public void setReutersBD(String reutersBD) {
		this.reutersBD = new File(reutersBD);
	}

}

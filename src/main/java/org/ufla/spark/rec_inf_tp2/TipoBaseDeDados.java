package org.ufla.spark.rec_inf_tp2;

/**
 * 
 * @author carlos
 * @author douglas
 * @author italo
 * 
 *         Enumeração dos tipos de base de dados Reuters 21578 que utilizamos.
 */
enum TipoBaseDeDados {

	/**
	 * Base de dados de treino, para realizar o treinamento do algoritmo de
	 * classificação.
	 */
	TREINO("training"),
	/**
	 * Base de dados de teste, para realizar a avaliação do algoritmo de
	 * classificação.
	 */
	TESTE("test"),
	/**
	 * Base de dados de treino em json salva em HDFS.
	 */
	TREINO_HDFS("training_hdfs"),
	/**
	 * Base de dados de teste em json salva em HDFS.
	 */
	TESTE_HDFS("test_hdfs");

	/**
	 * Nome do diretório onde este tipo da base de dados Reuters 21578 está salvo.
	 */
	private String nomeDiretorioBD;

	private TipoBaseDeDados(String nomeDiretorioBD) {
		this.nomeDiretorioBD = nomeDiretorioBD;
	}

	/**
	 * Recupera o nome do diretório onde este tipo da base de dados Reuters 21578
	 * está salvo.
	 * 
	 * @return nome do diretório onde este tipo da base de dados Reuters 21578 está
	 *         salvo
	 */
	public String getNomeDiretorioBD() {
		return nomeDiretorioBD;
	}

	/**
	 * Verifica se esse tipo de base de dados é original, as base de dados originais
	 * são TREINO e TESTE.
	 * 
	 * @return true se a base de dados é original, caso contrário false
	 */
	public boolean eOriginal() {
		switch (this) {
		case TREINO:
			return true;
		case TESTE:
			return true;
		default:
			return false;
		}
	}

	/**
	 * Transforma uma base de dados original (TREINO e TESTE) em json HDFS
	 * (TREINO_JSON_HDFS, TESTE_JSON_HDFS).
	 * 
	 * @return retorna a base de dados em json HDFS
	 */
	public TipoBaseDeDados paraJsonHDFS() {
		switch (this) {
		case TREINO:
			return TREINO_HDFS;
		case TESTE:
			return TESTE_HDFS;
		default:
			return this;
		}
	}

	/**
	 * Transforma uma base de dados em json HDFS (TREINO_JSON_HDFS, TESTE_JSON_HDFS)
	 * em original (TREINO e TESTE).
	 * 
	 * @return retorna a base de dados original
	 */
	public TipoBaseDeDados paraOriginal() {
		switch (this) {
		case TREINO_HDFS:
			return TREINO;
		case TESTE_HDFS:
			return TESTE;
		default:
			return this;
		}
	}
}
package org.ufla.spark.rec_inf_tp2;

/**
 * 
 * @author carlos
 * @author douglas
 * @author italo
 * 
 *         Enumeração dos tipos de extração de features que podem ser realizados
 *         nos dados.
 */
public enum ExtracaoFeatures {

	/**
	 * Sem tokenizar texto e extrair features.
	 */
	SEM_SELECAO("", 0),
	/**
	 * Extração de features usando HashingTF e IDF.
	 */
	HASHING_TF_IDF("_htfidf", 0),
	/**
	 * Extração de features usando HashingTF.
	 */
	HASHING_TF("_htf", 0);

	/**
	 * Sufixo do diretório onde a base de dados Reuters 21578 com esse tipo de
	 * selecao de features está salvo.
	 */
	private String sufixoDiretorioBD;
	/**
	 * Quantidade de features usadas, quando é 0 signfica que usou todas.
	 */
	private int qtdFeatures;

	private ExtracaoFeatures(String sufixoDiretorioBD, int qtdFeatures) {
		this.sufixoDiretorioBD = sufixoDiretorioBD;
		this.qtdFeatures = qtdFeatures;
	}

	/**
	 * Recupera a quantidade de features utilizadas, quando é 0 signfica que usou
	 * todas.
	 * 
	 * @return quantidade de features utilizadas.
	 */
	public int getQtdFeatures() {
		return qtdFeatures;
	}

	/**
	 * Define a quantidade de features utilizadas, quando é 0 signfica que usou
	 * todas.
	 * 
	 * @param qtdFeatures
	 *            quantidade de features utilizadas.
	 */
	public void setQtdFeatures(int qtdFeatures) {
		this.qtdFeatures = qtdFeatures;
	}

	/**
	 * Recupera o sufixo do diretório onde a base de dados Reuters 21578 com esse
	 * tipo de selecao de features está salvo.
	 * 
	 * @return sufixo do diretório onde a base de dados Reuters 21578 com esse tipo
	 *         de selecao de features está salvo
	 */
	public String getSufixoDiretorioBD() {
		return sufixoDiretorioBD + ((qtdFeatures > 0) ? String.valueOf(qtdFeatures) : "");
	}

}

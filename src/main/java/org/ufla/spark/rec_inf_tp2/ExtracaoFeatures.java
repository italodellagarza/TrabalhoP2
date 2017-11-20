package org.ufla.spark.rec_inf_tp2;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.Tokenizer;

import static org.ufla.spark.rec_inf_tp2.BaseDeDados.*;

/**
 * Enumeração dos tipos de extração de features que podem ser realizados nos
 * dados.
 * 
 * @author carlos
 * @author douglas
 * @author italo
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
	HASHING_TF("_htf", 0),
	/**
	 * Extração de features usando CountVectorizer para encontrar a frequência dos
	 * termos e IDF.
	 */
	COUNT_VECT_IDF("_countvecidf", 0);

	/**
	 * Sufixo do diretório onde a base de dados Reuters 21578 com esse tipo de
	 * selecao de features está salvo.
	 */
	private String sufixoDiretorioBD;
	/**
	 * Quantidade de features usadas, quando é 0 signfica que usou todas.
	 */
	private int qtdFeatures;
	/**
	 * Lista com as transformações necessárias para realizar a extração de features.
	 */
	private List<PipelineStage> transformacoesExtracaoFeatures;

	private ExtracaoFeatures(String sufixoDiretorioBD, int qtdFeatures) {
		this.sufixoDiretorioBD = sufixoDiretorioBD;
		this.qtdFeatures = qtdFeatures;
		criarTransformacoes();
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
		switch (this) {
		case HASHING_TF_IDF:
		case HASHING_TF:
			((HashingTF) transformacoesExtracaoFeatures.get(1)).setNumFeatures(qtdFeatures);
			break;
		case COUNT_VECT_IDF:
			((CountVectorizer) transformacoesExtracaoFeatures.get(1)).setVocabSize(qtdFeatures);
			break;
		default:
			break;
		}
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

	/**
	 * Recupera a lista com as transformações necessárias para realizar a extração
	 * de features.
	 * 
	 * @return lista com as transformações necessárias para realizar a extração de
	 *         features.
	 */
	public List<PipelineStage> getTransformacoes() {
		return transformacoesExtracaoFeatures;
	}

	/**
	 * Cria uma lista com as transformações da extração de features.
	 */
	private void criarTransformacoes() {
		transformacoesExtracaoFeatures = new ArrayList<>();
		Tokenizer tokenizer = new Tokenizer().setInputCol(CONTEUDO_FINAL_PRE_PROC_COL).setOutputCol(PALAVRAS_COL);
		transformacoesExtracaoFeatures.add(tokenizer);
		HashingTF hashingTF;
		CountVectorizer countVectorizer;
		IDF idf;
		switch (this.sufixoDiretorioBD) {
		case "_htfidf":
			hashingTF = new HashingTF().setInputCol(tokenizer.getOutputCol()).setOutputCol(FEATURES_COL);
			transformacoesExtracaoFeatures.add(hashingTF);
			break;
		case "_htf":
			hashingTF = new HashingTF().setInputCol(tokenizer.getOutputCol()).setOutputCol(CARACTERISTICAS_TF_COL);
			idf = new IDF().setInputCol(hashingTF.getOutputCol()).setOutputCol(FEATURES_COL);
			transformacoesExtracaoFeatures.add(hashingTF);
			transformacoesExtracaoFeatures.add(idf);
			break;
		case "_countvecidf":
			countVectorizer =
					new CountVectorizer().setInputCol(tokenizer.getOutputCol()).setOutputCol(CARACTERISTICAS_TF_COL);
			idf = new IDF().setInputCol(countVectorizer.getOutputCol()).setOutputCol(FEATURES_COL);
			System.out.println("VocabSize -> " + countVectorizer.getVocabSize());
			System.out.println("MinTF -> " + countVectorizer.getMinTF());
			System.out.println("MinDF -> " + countVectorizer.getMinDF() + "\n");
			transformacoesExtracaoFeatures.add(countVectorizer);
			transformacoesExtracaoFeatures.add(idf);
			break;
		case "":
			break;
		default:
			throw new RuntimeException("Tipo de extração de features não reconhecido.");
		}
	}

}

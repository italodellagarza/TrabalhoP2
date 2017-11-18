package org.ufla.spark.rec_inf_tp2;

import org.ufla.spark.rec_inf_tp2.transformacoes.TransformacaoGenerica;
import org.ufla.spark.rec_inf_tp2.transformacoes.TransformacaoMinuscula;
import org.ufla.spark.rec_inf_tp2.transformacoes.TransformacaoRemoverTags;
import org.ufla.spark.rec_inf_tp2.transformacoes.TransformacaoCodificacaoASCIIDesnec;
import org.ufla.spark.rec_inf_tp2.transformacoes.TransformacaoRemocaoStopWords;
import org.ufla.spark.rec_inf_tp2.transformacoes.TransformacaoStemmer;

import static org.ufla.spark.rec_inf_tp2.BaseDeDados.*;

/**
 * 
 * @author carlos
 * @author douglas
 * @author italo
 *
 *         Enumeração dos tipos de pré-processamento que podem ser realizados
 *         nos dados.
 */
enum PreProcessamento {
	/**
	 * Nenhum pré-processamento é realizado.
	 */
	NENHUM("", null, null, null),
	/**
	 * Todo pré-processamento é realizado. Transformação em letra minúsculas,
	 * remoção de tags, conversão para ASCII com remoção de pontos, números e
	 * caracteres especiais, remoção de stopwords e stemmer.
	 */
	@SuppressWarnings("unchecked")
	COMPLETO("_precproc_completo",
			new Class[] { TransformacaoMinuscula.class, TransformacaoRemoverTags.class,
					TransformacaoCodificacaoASCIIDesnec.class, TransformacaoRemocaoStopWords.class,
					TransformacaoStemmer.class },
			new String[] { CONTEUDO_COL, CONTEUDO_MINUSCULO_COL, CONTEUDO_MINUSCULO_TAGS_COL,
					CONTEUDO_MINUSCULO_TAGS_ASCII_COL, CONTEUDO_SEM_STOP_WORDS_COL },
			new String[] { CONTEUDO_MINUSCULO_COL, CONTEUDO_MINUSCULO_TAGS_COL, CONTEUDO_MINUSCULO_TAGS_ASCII_COL,
					CONTEUDO_SEM_STOP_WORDS_COL, CONTEUDO_FINAL_PRE_PROC_COL });

	/**
	 * Sufixo do diretório onde a base de dados Reuters 21578 com esse tipo de
	 * pré-processamento está salvo.
	 */
	private String sufixoDiretorioBD;
	/**
	 * Classes de pré-processamento que devem ser aplicadas neste pré-processamento,
	 * em ordem de aplicação.
	 */
	@SuppressWarnings("rawtypes")
	private Class<? extends TransformacaoGenerica>[] classesTransformacao;
	/**
	 * Colunas referentes as entradas dos pré-processamentos.
	 */
	private String[] colunasEntrada;
	/**
	 * Colunas referentes as saídas dos pré-processamentos.
	 */
	private String[] colunasSaida;

	@SuppressWarnings("rawtypes")
	private PreProcessamento(String sufixoDiretorioBD, Class<? extends TransformacaoGenerica>[] classesTransformacao,
			String[] colunasEntrada, String[] colunasSaida) {
		this.sufixoDiretorioBD = sufixoDiretorioBD;
		this.classesTransformacao = classesTransformacao;
		this.colunasEntrada = colunasEntrada;
		this.colunasSaida = colunasSaida;
	}

	/**
	 * Recupera o nome do diretório onde este tipo da base de dados Reuters 21578
	 * está salvo.
	 * 
	 * @return nome do diretório onde este tipo da base de dados Reuters 21578 está
	 *         salvo
	 */
	public String getSufixoDiretorioBD() {
		return sufixoDiretorioBD;
	}

	/**
	 * Recupera as classes de pré-processamento que devem ser aplicadas neste
	 * pré-processamento, em ordem de aplicação.
	 * 
	 * @return classes de pré-processamento que devem ser aplicadas neste
	 *         pré-processamento, em ordem de aplicação
	 */
	@SuppressWarnings("rawtypes")
	public Class<? extends TransformacaoGenerica>[] getClassesTransformacao() {
		return classesTransformacao;
	}

	/**
	 * Recupera as colunas referentes as entradas dos pré-processamentos.
	 * 
	 * @return colunas referentes as entradas dos pré-processamentos.
	 */
	public String[] getColunasEntrada() {
		return colunasEntrada;
	}

	/**
	 * Recupera as colunas referentes as saídas dos pré-processamentos.
	 * 
	 * @return colunas referentes as saídas dos pré-processamentos
	 */
	public String[] getColunasSaida() {
		return colunasSaida;
	}
}
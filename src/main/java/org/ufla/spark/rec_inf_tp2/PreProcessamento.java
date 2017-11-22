package org.ufla.spark.rec_inf_tp2;

import static org.ufla.spark.rec_inf_tp2.BaseDeDados.CONTEUDO_COL;
import static org.ufla.spark.rec_inf_tp2.BaseDeDados.CONTEUDO_FINAL_PRE_PROC_COL;
import static org.ufla.spark.rec_inf_tp2.BaseDeDados.CONTEUDO_MINUSCULO_COL;
import static org.ufla.spark.rec_inf_tp2.BaseDeDados.CONTEUDO_MINUSCULO_TAGS_ASCII_COL;
import static org.ufla.spark.rec_inf_tp2.BaseDeDados.CONTEUDO_MINUSCULO_TAGS_COL;
import static org.ufla.spark.rec_inf_tp2.BaseDeDados.CONTEUDO_SEM_STOP_WORDS_COL;

import org.apache.spark.ml.Transformer;
import org.apache.spark.sql.types.StructType;
import org.ufla.spark.rec_inf_tp2.transformacoes.TransformacaoCodificacaoASCIIDesnec;
import org.ufla.spark.rec_inf_tp2.transformacoes.TransformacaoGenerica;
import org.ufla.spark.rec_inf_tp2.transformacoes.TransformacaoMinuscula;
import org.ufla.spark.rec_inf_tp2.transformacoes.TransformacaoRemocaoStopWords;
import org.ufla.spark.rec_inf_tp2.transformacoes.TransformacaoRemoverTags;
import org.ufla.spark.rec_inf_tp2.transformacoes.TransformacaoStemmer;

/**
 * Enumeração dos tipos de pré-processamento que podem ser realizados nos dados.
 * 
 * @author carlos
 * @author douglas
 * @author italo
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
	 * Array com as transformações de pré-processamento.
	 */
	private Transformer[] transformacoes;

	@SuppressWarnings("rawtypes")
	private PreProcessamento(String sufixoDiretorioBD, Class<? extends TransformacaoGenerica>[] classesTransformacao,
			String[] colunasEntrada, String[] colunasSaida) {
		this.sufixoDiretorioBD = sufixoDiretorioBD;
		criarPreProcessamentos(classesTransformacao, colunasEntrada, colunasSaida);
	}

	/**
	 * Cria o array com as transformações de um determinado pré-processamento.
	 * 
	 * @param classesTransformacao
	 *            classes de pré-processamento que devem ser aplicadas neste
	 *            pré-processamento, em ordem de aplicação.
	 * @param colunasEntrada
	 *            colunas referentes as entradas dos pré-processamentos.
	 * @param colunasSaida
	 *            colunas referentes as saídas dos pré-processamentos.
	 * @return array com as transformações de pré-processamento
	 */
	@SuppressWarnings("rawtypes")
	private void criarPreProcessamentos(Class<? extends TransformacaoGenerica>[] classesTransformacao,
			String[] colunasEntrada, String[] colunasSaida) {
		if (classesTransformacao == null) {
			transformacoes = null;
			return;
		}
		validar(classesTransformacao, colunasEntrada, colunasSaida);
		transformacoes = new Transformer[classesTransformacao.length];
		StructType esquemaAtual = BaseDeDados.getInstancia().criarEsquemaOriginal();
		for (int i = 0; i < classesTransformacao.length; i++) {
			if (!classesTransformacao[i].getSuperclass().equals(TransformacaoGenerica.class)) {
				throw new RuntimeException("ERRO! A classe " + classesTransformacao[i].getName()
						+ " de pré-processamento não é filha de TransformacaoGenerica.");
			}
			try {
				transformacoes[i] = classesTransformacao[i].newInstance().setColunaEntrada(colunasEntrada[i])
						.setColunaSaida(colunasSaida[i]).setEsquemaEntrada(esquemaAtual);
			} catch (InstantiationException | IllegalAccessException e) {
				throw new RuntimeException("ERRO! Não foi possível instanciar objeto da classe "
						+ classesTransformacao[i].getName() + " com o construtor padrão.");
			}
			esquemaAtual = transformacoes[i].transformSchema(esquemaAtual);
		}
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
	 * Recupera o array com as transformações de pré-processamento.
	 * 
	 * @return array com as transformações de pré-processamento
	 */
	public Transformer[] getTransformacoes() {
		return transformacoes;
	}

	/**
	 * Verifica se os dados para criar as transformações de pré-processamento são
	 * válidas, se não são válidas lança uma exceção de tempo de execução e a
	 * aplicação para.
	 * 
	 * @param classesTransformacao
	 *            classes de pré-processamento que devem ser aplicadas neste
	 *            pré-processamento, em ordem de aplicação.
	 * @param colunasEntrada
	 *            colunas referentes as entradas dos pré-processamentos.
	 * @param colunasSaida
	 *            colunas referentes as saídas dos pré-processamentos.
	 */
	@SuppressWarnings("rawtypes")
	private void validar(Class<? extends TransformacaoGenerica>[] classesTransformacao, String[] colunasEntrada,
			String[] colunasSaida) {
		if (colunasEntrada == null || colunasSaida == null) {
			throw new RuntimeException("ERRO! colunasEntrada ou colunasSaida é null.");
		}
		if (classesTransformacao.length != colunasEntrada.length
				|| classesTransformacao.length != colunasSaida.length) {
			throw new RuntimeException(String.format(
					"ERRO! Tamanho dos arrays classesTransformacao (%d), colunasEntrada (%d), colunasSaida (%d) são diferentes.",
					classesTransformacao.length, colunasEntrada.length, colunasSaida.length));
		}
		if (!colunasEntrada[0].equals(CONTEUDO_COL)) {
			throw new RuntimeException(
					String.format("ERRO! colunaEntrada[0] (%s) é diferente de %s.", colunasEntrada[0], CONTEUDO_COL));
		}
		if (!colunasSaida[colunasSaida.length - 1].equals(CONTEUDO_FINAL_PRE_PROC_COL)) {
			throw new RuntimeException(
					String.format("ERRO! colunasSaida[colunasSaida.length - 1] (%s) é diferente de %s.",
							colunasSaida[colunasSaida.length - 1], CONTEUDO_FINAL_PRE_PROC_COL));
		}
	}

}
package org.ufla.spark.rec_inf_tp2.transformacoes;

import java.io.IOException;
import java.util.Set;

import org.ufla.spark.rec_inf_tp2.funcoes.FunRemocaoStopWords;
import org.ufla.spark.rec_inf_tp2.funcoes.FunStemmer;
import org.ufla.spark.rec_inf_tp2.utils.StopWords;

public class TransformacaoRemocaoStopWords extends TransformacaoGenerica<FunRemocaoStopWords> {

	private static final long serialVersionUID = 1L;

	private Set<String> setStopWords;

	@Override
	protected String getLabeluid() {
		return "TransformacaoRemocaoStopWords";
	}

	@Override
	public TransformacaoGenerica<FunStemmer> criarTransformacao() {
		return new TransformacaoStemmer();
	}

	@Override
	public FunRemocaoStopWords criarFuncao(int indiceColEntrada, int indiceColSaida) {
		if (setStopWords == null) {
			try {
				setStopWords = new StopWords().getSetStopWords();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return new FunRemocaoStopWords(indiceColEntrada, indiceColSaida, setStopWords);
	}

}

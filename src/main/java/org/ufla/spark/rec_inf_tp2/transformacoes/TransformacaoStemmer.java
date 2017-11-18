package org.ufla.spark.rec_inf_tp2.transformacoes;

import org.ufla.spark.rec_inf_tp2.funcoes.FunStemmer;

public class TransformacaoStemmer extends TransformacaoGenerica<FunStemmer> {

	private static final long serialVersionUID = 1L;
	
	@Override
	protected String getLabeluid() {
		return "TransformacaoStemmer";
	}

	@Override
	public TransformacaoGenerica<FunStemmer> criarTransformacao() {
		return new TransformacaoStemmer();
	}

	@Override
	public FunStemmer criarFuncao(int colunaEntrada, int indiceColSaida) {
		return new FunStemmer(colunaEntrada, indiceColSaida);
	}

}

package org.ufla.spark.rec_inf_tp2.transformacoes;

import org.ufla.spark.rec_inf_tp2.funcoes.FunMapMinuscula;

public class TransformacaoMinuscula extends TransformacaoGenerica<FunMapMinuscula> {

	private static final long serialVersionUID = 1L;
	
	@Override
	protected String getLabeluid() {
		return "TransformacaoMinuscula";
	}

	@Override
	public TransformacaoGenerica<FunMapMinuscula> criarTransformacao() {
		return new TransformacaoMinuscula();
	}

	@Override
	public FunMapMinuscula criarFuncao(int indiceColEntrada, int indiceColSaida) {
		return new FunMapMinuscula(indiceColEntrada, indiceColSaida);
	}

}

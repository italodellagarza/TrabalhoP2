package org.ufla.spark.rec_inf_tp2.transformacoes;

import org.ufla.spark.rec_inf_tp2.funcoes.FunRemoverTags;

public class TransformacaoRemoverTags extends TransformacaoGenerica<FunRemoverTags> {

	private static final long serialVersionUID = 1L;

	@Override
	protected String getLabeluid() {
		return "TransformacaoRemoverTags";
	}

	@Override
	public TransformacaoGenerica<FunRemoverTags> criarTransformacao() {
		return new TransformacaoRemoverTags();
	}

	@Override
	public FunRemoverTags criarFuncao(int indiceColEntrada, int indiceColSaida) {
		return new FunRemoverTags(indiceColEntrada, indiceColSaida);
	}

}

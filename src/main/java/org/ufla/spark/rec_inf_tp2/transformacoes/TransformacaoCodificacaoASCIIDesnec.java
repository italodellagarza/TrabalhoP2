package org.ufla.spark.rec_inf_tp2.transformacoes;

import org.ufla.spark.rec_inf_tp2.funcoes.FunCodificacaoASCIIDesnec;

public class TransformacaoCodificacaoASCIIDesnec extends TransformacaoGenerica<FunCodificacaoASCIIDesnec> {

	private static final long serialVersionUID = 1L;

	@Override
	protected String getLabeluid() {
		return "TransformacaoCodificacaoASCIIDesnec";
	}

	@Override
	public TransformacaoGenerica<FunCodificacaoASCIIDesnec> criarTransformacao() {
		return new TransformacaoCodificacaoASCIIDesnec();
	}

	@Override
	public FunCodificacaoASCIIDesnec criarFuncao(int indiceColEntrada, int indiceColSaida) {
		return new FunCodificacaoASCIIDesnec(indiceColEntrada, indiceColSaida);
	}

}

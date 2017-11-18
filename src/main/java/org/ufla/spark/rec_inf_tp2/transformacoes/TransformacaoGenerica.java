package org.ufla.spark.rec_inf_tp2.transformacoes;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.ufla.spark.rec_inf_tp2.utils.DataSetUtils;

public abstract class TransformacaoGenerica<F extends MapFunction<Row, Row>> extends Transformer {

	private static final long serialVersionUID = 1L;
	private String colunaEntrada;
	private String colunaSaida;
	private StructType esquemaAntigo;
	protected StructType novoEsquema;
	private String uid;

	private static int n = 0;

	protected abstract String getLabeluid();

	@Override
	public String uid() {
		return uid;
	}

	public TransformacaoGenerica() {
		uid = getLabeluid() + "_" + this.hashCode() + "_" + n;
		n++;
	}

	public abstract TransformacaoGenerica<?> criarTransformacao();

	public abstract F criarFuncao(int indiceColEntrada, int indiceColSaida);

	@Override
	public Transformer copy(ParamMap arg0) {
		TransformacaoGenerica<?> copia = criarTransformacao().setColunaEntrada(colunaEntrada)
				.setColunaSaida(colunaSaida).setEsquemaAntigo(esquemaAntigo);
		copia.novoEsquema = novoEsquema;
		return copia;
	}

	public boolean eValido(int qtdColunas, int indiceColEntrada, int indiceColSaida) {
		return indiceColEntrada >= 0 && indiceColEntrada < qtdColunas && indiceColSaida >= 0
				&& indiceColSaida <= qtdColunas;
	}

	public void criarNovoEsquema() {
		if (esquemaAntigo == null) {
			throw new RuntimeException("Esquema antigo não foi definido em TransformaçãoMinúscula!");
		}
		int indiceColEntrada = DataSetUtils.getIndiceColuna(esquemaAntigo, colunaEntrada);
		int indiceColSaida = DataSetUtils.getIndiceColuna(esquemaAntigo, colunaSaida);
		int qtdColunas = esquemaAntigo.length();
		if (!eValido(qtdColunas, indiceColEntrada, indiceColSaida)) {
			throw new RuntimeException("Coluna de entrada/saida incorreta em TransformaçãoMinúscula!");
		}
		int novaQtdColunas = qtdColunas;
		if (qtdColunas == indiceColSaida) {
			novaQtdColunas++;
		}
		StructField[] novosCampos = new StructField[novaQtdColunas];
		StructField[] antigosCampos = esquemaAntigo.fields();
		StructField estruturaSaida = DataTypes.createStructField(colunaSaida, DataTypes.StringType, true);
		for (int i = 0; i < qtdColunas; i++) {
			novosCampos[i] = antigosCampos[i];
		}
		novosCampos[indiceColSaida] = estruturaSaida;
		novoEsquema = DataTypes.createStructType(novosCampos);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Dataset<Row> transform(Dataset<?> dataset) {
		Dataset<Row> datasetRow = (Dataset<Row>) dataset;
		esquemaAntigo = datasetRow.schema();
		criarNovoEsquema();
		int indiceColEntrada = DataSetUtils.getIndiceColuna(esquemaAntigo, colunaEntrada);
		int indiceColSaida = DataSetUtils.getIndiceColuna(esquemaAntigo, colunaSaida);
		return dataset.sparkSession().createDataFrame(
				datasetRow.map(criarFuncao(indiceColEntrada, indiceColSaida), Encoders.bean(Row.class)).rdd(),
				novoEsquema);
	}

	@Override
	public StructType transformSchema(StructType arg0) {
		if (novoEsquema == null) {
			criarNovoEsquema();
		}
		return novoEsquema;
	}

	public String getColunaEntrada() {
		return colunaEntrada;
	}

	public TransformacaoGenerica<?> setColunaEntrada(String colunaEntrada) {
		this.colunaEntrada = colunaEntrada;
		return this;
	}

	public String getColunaSaida() {
		return colunaSaida;
	}

	public TransformacaoGenerica<?> setColunaSaida(String colunaSaida) {
		this.colunaSaida = colunaSaida;
		return this;
	}

	public StructType getEsquemaAntigo() {
		return esquemaAntigo;
	}

	public TransformacaoGenerica<?> setEsquemaAntigo(StructType esquemaAntigo) {
		this.esquemaAntigo = esquemaAntigo;
		return this;
	}
}
package org.ufla.spark.rec_inf_tp2.funcoes;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import scala.Tuple2;

/**
 * 
 * @author carlos
 * 
 *         Responsável por realizar a inserção da classe de um documento em sua
 *         linha (Row).
 */
public class FunInsereClasse implements Function<Tuple2<String, String>, Row> {

	private static final long serialVersionUID = 1L;

	/**
	 * Classe do documento.
	 */
	private String classe;
	private Integer codigoClasse;

	public FunInsereClasse(String classe, int codigoClasse) {
		this.classe = classe;
		this.codigoClasse = codigoClasse;
	}

	/**
	 * Retorna a linha com a inserção da classe do objeto.
	 */
	@Override
	public Row call(Tuple2<String, String> tuple2) throws Exception {
		Object[] campos = new Object[4];
		campos[0] = tuple2._1;
		campos[1] = tuple2._2;
		campos[2] = classe;
		campos[3] = codigoClasse;
		return RowFactory.create(campos);
	}

}

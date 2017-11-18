package org.ufla.spark.rec_inf_tp2.utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

public class DataSetUtils {

	public static int getIndiceColuna(Dataset<Row> dataset, String coluna) {
		return getIndiceColuna(dataset.schema(), coluna);
	}

	public static int getIndiceColuna(StructType esquema, String coluna) {
		String[] colunas = esquema.fieldNames();
		for (int i = 0; i < colunas.length; i++) {
			if (colunas[i].equals(coluna)) {
				return i;
			}
		}
		return colunas.length;
	}
	
	public static void printRow(Row row) {
		System.out.println("\n----PRINT ROW----");
		for (int i = 0; i < row.length(); i++) {
			System.out.println("<<<");
			System.out.println(row.get(i));
			System.out.println(">>>");
		}
		System.out.println("\n----END PRINT ROW----\n");
	}

}

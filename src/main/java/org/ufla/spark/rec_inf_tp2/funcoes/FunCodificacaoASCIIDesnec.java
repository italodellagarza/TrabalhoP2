package org.ufla.spark.rec_inf_tp2.funcoes;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class FunCodificacaoASCIIDesnec implements MapFunction<Row, Row> {

	private static final long serialVersionUID = 1L;

	/**
	 * Coluna em que deve aplicar o pré-processamento.
	 */
	private int colunaEntrada;
	private int colunaSaida;

	private static Map<Character, Character> paraASCII;

	static {
		paraASCII = new HashMap<>();
		paraASCII.put('ç', 'c');
		paraASCII.put('ć', 'c');
		paraASCII.put('ĉ', 'c');
		paraASCII.put('ź', 'z');
		paraASCII.put('ẑ', 'z');
		paraASCII.put('ǘ', 'v');
		paraASCII.put('ǜ', 'v');
		paraASCII.put('ṽ', 'v');
		paraASCII.put('ù', 'u');
		paraASCII.put('ú', 'u');
		paraASCII.put('ũ', 'u');
		paraASCII.put('û', 'u');
		paraASCII.put('ñ', 'n');
		paraASCII.put('ń', 'n');
		paraASCII.put('ǹ', 'n');
		paraASCII.put('ḿ', 'm');
		paraASCII.put('á', 'a');
		paraASCII.put('à', 'a');
		paraASCII.put('â', 'a');
		paraASCII.put('ã', 'a');
		paraASCII.put('õ', 'o');
		paraASCII.put('ô', 'o');
		paraASCII.put('ö', 'o');
		paraASCII.put('ò', 'o');
		paraASCII.put('ó', 'o');
		paraASCII.put('ś', 's');
		paraASCII.put('ŝ', 's');
		paraASCII.put('ǵ', 'g');
		paraASCII.put('ĝ', 'g');
		paraASCII.put('ĥ', 'h');
		paraASCII.put('ĵ', 'j');
		paraASCII.put('ḱ', 'k');
		paraASCII.put('ĺ', 'l');
		paraASCII.put('ḉ', 'c');
		paraASCII.put('ẃ', 'w');
		paraASCII.put('ẁ', 'w');
		paraASCII.put('ŵ', 'w');
		paraASCII.put('é', 'e');
		paraASCII.put('è', 'e');
		paraASCII.put('ê', 'e');
		paraASCII.put('ẽ', 'e');
		paraASCII.put('ŕ', 'r');
		paraASCII.put('ý', 'y');
		paraASCII.put('ỳ', 'y');
		paraASCII.put('ỹ', 'y');
		paraASCII.put('ŷ', 'y');
		paraASCII.put('ú', 'u');
		paraASCII.put('ù', 'u');
		paraASCII.put('û', 'u');
		paraASCII.put('ũ', 'u');
		paraASCII.put('ĩ', 'i');
		paraASCII.put('î', 'i');
		paraASCII.put('í', 'i');
		paraASCII.put('ì', 'i');
		paraASCII.put('ṕ', 'p');
		paraASCII.put('ẅ', 'w');
		paraASCII.put('ë', 'e');
		paraASCII.put('ẗ', 't');
		paraASCII.put('ÿ', 'y');
		paraASCII.put('ü', 'u');
		paraASCII.put('ï', 'i');
		paraASCII.put('ä', 'a');
		paraASCII.put('ḧ', 'h');
		paraASCII.put('ẍ', 'x');
	}

	public FunCodificacaoASCIIDesnec(int colunaEntrada, int colunaSaida) {
		this.colunaEntrada = colunaEntrada;
		this.colunaSaida = colunaSaida;
	}

	@SuppressWarnings("unused")
	private boolean eDigito(char c) {
		return c >= '0' && c <= '9';
	}

	private boolean eLetra(char c) {
		return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
	}

	@Override
	public Row call(Row row) throws Exception {
		int n = row.length();
		if (n == colunaSaida) {
			n++;
		}
		Object[] campos = new Object[n];
		for (int i = 0; i < row.length(); i++) {
			campos[i] = row.get(i);
		}

		char[] entrada = row.getString(colunaEntrada).toCharArray();
		StringBuilder saida = new StringBuilder();
		boolean espaco = false;
		for (int i = 0; i < entrada.length; i++) {
			if (eLetra(entrada[i])) {
				if (espaco) {
					saida.append(' ');
				}
				saida.append(entrada[i]);
				espaco = false;
				continue;
			}
			entrada[i] = paraASCII.getOrDefault(entrada[i], ' ');
			if (Character.isWhitespace(entrada[i])) {
				if (!espaco) {
					espaco = true;
				}
			} else {
				if (espaco) {
					saida.append(' ');
				}
				saida.append(entrada[i]);
				espaco = false;
			}
		}
		campos[colunaSaida] = saida.toString();
		return RowFactory.create(campos);
	}

}

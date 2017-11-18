package org.ufla.spark.rec_inf_tp2.extrator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.ufla.spark.rec_inf_tp2.extrator.Documento;

/**
 * Classe responsável por implementar métodos genéricos para extrair dados de um
 * arquivo.
 * 
 * @author carlos
 * @author douglas
 * @author italo
 */

public class ExtratorDocumentos {
	/**
	 * Path de consultas que está sendo processado.
	 */
	private static File dir;

	/**
	 * Leitor do arquivo atual.
	 */
	private static BufferedReader br;

	/**
	 * Última linha que foi lida do arquivo atual.
	 */
	private static String linhaAtual;

	/**
	 * Caractere especial que está contido em alguns arquivos da base de dados para
	 * marcar a sua última linha.
	 */
	private static final char FINAL = '';

	/**
	 * Lista de documentos para Treino
	 */
	private static List<Documento> documentosTraining = new ArrayList<>();

	/**
	 * Lista de documentos para Teste
	 */
	private static List<Documento> documentosTest = new ArrayList<>();

	public static void extrator() {
		dir = new File("Reuters21578-Apte-90Cat");
		File[] split = dir.listFiles();
		double idClasse;

		for (File spl : split) {
			if (spl.getName().toLowerCase().equals("training")) {
				idClasse = 0.0;
				File[] topicos = spl.listFiles();
				for (File top : topicos) {
					idClasse++;
					File[] arquivos = top.listFiles();
					for (File arquivo : arquivos) {
						setArquivo(arquivo);
						Documento doc = new Documento(idClasse, spl.getName().toLowerCase(),
								top.getName().toLowerCase(), leLinhas().toString().toLowerCase());
						documentosTraining.add(doc);
					}
				}
			} else {
				idClasse = 0.0;
				File[] topicos = spl.listFiles();
				for (File top : topicos) {
					File[] arquivos = top.listFiles();
					idClasse++;
					for (File arquivo : arquivos) {
						setArquivo(arquivo);
						Documento doc = new Documento(idClasse, spl.getName().toLowerCase(),
								top.getName().toLowerCase(), leLinhas().toString().toLowerCase());
						documentosTest.add(doc);
					}
				}
			}
		}
	}

	public static boolean setArquivo(File arquivo) {
		if (arquivo == null) {
			return false;
		}

		try {
			br = new BufferedReader(new FileReader(arquivo));
		} catch (Exception e) {
			System.out.println("Arquivo não existe!");
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
		return true;
	}

	/**
	 * Lê uma linha do arquivo atual e salva na variável de instância linhaAtual.
	 * Esse método trata a exceção de leitura apenas imprimindo sua pilha de
	 * execução e a relançando como exceção de runtime (não verificada). Este
	 * processo é realizado, pois os arquivos são controlados e acredita que não
	 * será lançada exceção.
	 */
	public static StringBuilder leLinhas() {
		try {
			linhaAtual = br.readLine();
			// numLinhaAtual++;
			StringBuilder valor = new StringBuilder();

			if (linhaAtual != null && !linhaAtual.trim().isEmpty()) {
				valor = new StringBuilder(linhaAtual.trim());
			}

			while (!isFinalDeArquivo()) {
				linhaAtual = br.readLine();
				// numLinhaAtual++;
				if (linhaAtual != null && !linhaAtual.trim().isEmpty()) {
					valor.append(" ").append(linhaAtual.trim());
				}
			}
			// numLinhaAtual=0;
			fechaLeitor();
			return valor.replace(0, 1, "");
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
	}

	/**
	 * Verifica se a leitura do arquivo chegou ao fim dele.
	 * 
	 * @return true, se chegou ao fim do arquivo, caso contrário, false
	 */
	public static boolean isFinalDeArquivo() {
		// fim de arquivo normal com linha em branco
		if (linhaAtual == null) {
			return true;
		}
		// fim de arquivo com linha com caracteres especiais
		for (char c : linhaAtual.toCharArray()) {
			if (c != FINAL) {
				return false;
			}
		}
		return linhaAtual == null;
	}

	/**
	 * Fecha o leitor que está sendo utilizado para ler o arquivo.
	 */
	public static void fechaLeitor() {
		try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
	}

	public static List<Documento> getDocumentosTraining() {
		return documentosTraining;
	}

	public static List<Documento> getDocumentosTest() {
		return documentosTest;
	}
}

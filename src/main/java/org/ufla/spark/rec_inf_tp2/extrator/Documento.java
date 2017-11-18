package org.ufla.spark.rec_inf_tp2.extrator;

public class Documento {

	private double idClasse;
	private String split;
	private String topico;
	private String conteudo;

	public Documento(double idClasse, String split, String topico, String content) {
		this.idClasse = idClasse;
		this.split = split;
		this.topico = topico;
		this.conteudo = content;
	}

	public double getIdClasse() {
		return idClasse;
	}

	public String getTopico() {
		return topico;
	}

	public String getConteudo() {
		return conteudo;
	}

	public void setConteudo(String conteudo) {
		this.conteudo = conteudo;
	}

	public String getSplit() {
		return this.split;
	}

	@Override
	public String toString() {
		return "Documento [idClasse=" + idClasse + ", topico=" + topico + ", conteudo=" + conteudo + ", split=" + split
				+ "]";
	}

}

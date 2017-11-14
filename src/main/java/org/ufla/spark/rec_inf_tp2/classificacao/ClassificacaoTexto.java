package org.ufla.spark.rec_inf_tp2.classificacao;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.ufla.spark.rec_inf_tp2.extrator.Documento;
import org.ufla.spark.rec_inf_tp2.extrator.ExtratorDocumentos;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ClassificacaoTexto {
	
	private final static String TEST = "test";
    private final static String TRAINING = "training";
	
	/**
     * Returns a subset of topics that has both test and training data.
     */
    public static Set<String> getTopicsForClassification(List<Documento> documentos) {
        ImmutableSet.Builder<String> training = ImmutableSet.builder();
        ImmutableSet.Builder<String> test = ImmutableSet.builder();
        for (Documento doc : documentos) {
        	
            if (doc.getSplit().equals(TRAINING)) {
                training.add(doc.getTopico());
            } else if (doc.getSplit().equals(TEST)) {
                test.add(doc.getTopico());
            }
        }
        return Sets.intersection(training.build(), test.build());
        
    }
    
    public static List<Modelo> getClassificationModels(List<Documento> documentos, double f1Threshold) {
        Set<String> topics = getTopicsForClassification(documentos);
        Map<String, Modelo.Builder> modelBuilders = Maps.newHashMap();
        
        for (String topic : topics) {
            modelBuilders.put(topic, new Modelo.Builder());
        }

        // Tokenize data for building.
        HashingTF tf = new HashingTF(65536);
        for (Documento doc : documentos) {
            Vector vector = tf.transform(Tokenizer.getTokens(doc));
            
            for (String topic : topics) {
                Modelo.Builder modelBuilder = modelBuilders.get(topic);
                double label = doc.getTopico().contains(topic) ? 1.0 : 0.0;
                modelBuilder.add(doc.getSplit(), new LabeledPoint(label, vector));
            }
        }

        // Now build, evaluate and prune the models.
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        ImmutableList.Builder<Modelo> resultBuilder = ImmutableList.builder();
        try (JavaSparkContext context = new JavaSparkContext("local[2]", "ClassificacaoTexto")) {
            for (Entry<String, Modelo.Builder> e : modelBuilders.entrySet()) {
                Modelo model = e.getValue().build(context, e.getKey());
                if (model.getF1() > f1Threshold) {
                    resultBuilder.add(model);
                    // FIXME If desired it is possible to store the models using the following line
                   // model.getModel().save(context.sc(), "target/tmp/" + model.getTopic() + "_NB");
                }
            }
            context.stop();
        }

        return resultBuilder.build();
    }

    public static void main(String[] args) {
    	ExtratorDocumentos.extrator();
    	List<Documento> documentos = new ArrayList<>(); 
    	documentos.addAll(ExtratorDocumentos.getDocumentosTraining());
    	documentos.addAll(ExtratorDocumentos.getDocumentosTest());
        List<Modelo> models = getClassificationModels(documentos, 0.1);       
        for (Modelo m : models) {
            System.out.println(m);
        }
    }
    
    
}

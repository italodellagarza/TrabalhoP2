import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;


public class Main {

	public static void main(String[] args) {
		SparkConf sc = new SparkConf().setAppName("Teste");
		JavaSparkContext ctx = new JavaSparkContext(sc);
		String logFile = "/home/italo/spark-2.2.0-bin-hadoop2.7/README.md"; // Should be some file on your system
	    JavaRDD<String> logData = ctx.textFile(logFile);

	    long numAs = logData.filter(s -> s.contains("a")).count();
	    long numBs = logData.filter(s -> s.contains("b")).count();

	    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
		ctx.close();

	}

}

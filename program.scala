//spark-shell -i program.scala
//Demo.main()
//package ml.lsdp.example
//sc.setLogLevel("ERROR") usunięcie info log
//spark-shell -i program2.scala --packages ch.cern.sparkmeasure:spark-measure_2.11:0.13
//val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark) 
//stageMetrics.runAndMeasure(spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show())



import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.mllib.random.RandomRDDs._
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.sql.Encoders
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.BinaryLogisticRegressionSummary
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row
import spark.implicits._


object Demo {

case class Skin(B: Int, G: Int, R:Int, Class:Int) //kolory BGR

def main(): Unit ={

  //val log = LoggerFactory.getLogger(this.getClass)

  val spark = SparkSession //builder
    .builder
    .appName("Spark Demo rekombinacja")
    .master("local") //bt
    .getOrCreate()

import spark.implicits._

val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark)

  val SkinSchema = Encoders.product[Skin].schema
  val Data = spark.read.format("csv").option("header","false").schema(SkinSchema).load("Skin_NonSkin.txt")
  Data.show()
  Data.printSchema() //pokaż schema

  stageMetrics.runAndMeasure(Data.describe().show())
  //spark.time(Data.describe().show())


  println("Percentile:")
  Data.selectExpr("percentile(B, 0.95)").show()
  Data.stat.freqItems(Seq("B"), 0.5).show() //częsciej niż 50%
  val count1 = Data.select("B").distinct().count() //count of unique values dla B
  println("count of unique values (B):")
  println(count1)
  val count2 = Data.distinct().count() //count of unique values dla wszystkich
  println("count of unique values (All):")
  println(count2)
  println("counts for each value:")
  Data.groupBy("B").count().show() //counts for each value
  println("Most common and uncommon [value,times]:")
  println(Data.groupBy("B").count().orderBy($"count".desc).first)
  println(Data.groupBy("B").count().orderBy($"count".asc).first)


  val features = Array("B","G","R")
  val assembler = new VectorAssembler().setInputCols(features).setOutputCol("features")
  var Data2 = assembler.transform(Data)

  val labelIndexer = new StringIndexer().setInputCol("Class").setOutputCol("label")
  Data2 = labelIndexer.fit(Data2).transform(Data2)

  val Array(trainingData, testData) = Data2.randomSplit(Array(0.8, 0.2))

  //val model = new LogisticRegression().fit(Data2)

  val lr = new LogisticRegression()//.setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
  val model = lr.fit(trainingData)    
  //println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

  val trainingSummary = model.summary
  println("Metryki wbudowane:")
  val accuracy = trainingSummary.accuracy
  val falsePositiveRate = trainingSummary.weightedFalsePositiveRate
  val truePositiveRate = trainingSummary.weightedTruePositiveRate
  val fMeasure = trainingSummary.weightedFMeasure
  val precision = trainingSummary.weightedPrecision
  val recall = trainingSummary.weightedRecall
  println(s"Accuracy: $accuracy\nFPR: $falsePositiveRate\nTPR: $truePositiveRate\n" +
  s"F-measure: $fMeasure\nPrecision: $precision\nRecall: $recall")

  val predictions = model.transform(trainingData)
  //nowe kolumny rawPrediction probablity prediction
  //predictions.show


//Reszta metryk
  val sel = predictions.select("label", "prediction")
  val total = predictions.count()
  val correct = sel.filter($"label" === $"prediction").count()
  val wrong = sel.filter(not($"label" === $"prediction")).count()
  val truep = sel.filter($"prediction" === 1.0).filter($"label" === $"prediction").count()
  val truen = sel.filter($"prediction" === 0.0).filter($"label" === $"prediction").count()
  val falsen = sel.filter($"prediction" === 0.0).filter(not($"label" === $"prediction")).count()
  val falsep = sel.filter($"prediction" === 1.0).filter(not($"label" === $"prediction")).count()


  println("\nTotal:", total)
  println("Correct:", correct)
  println("Wrong:", wrong)
  println("TruePositive:", truep)
  println("TrueNegative:", truen)
  println("FalseNegative:", falsen)
  println("FalsePositive:", falsep)

  val accuracy2 = correct.toDouble/total.toDouble
  val precision2 = truep.toDouble/(truep.toDouble+falsep.toDouble)
  val recall2 = truep.toDouble/(truep.toDouble+falsen.toDouble)
  val f1score2 = 2.0*(recall2 * precision2)/(recall2 + precision2)

  println("\nMetryki_2:")
  println("Accuracy:", accuracy2)
  println("Precision:", precision2)
  println("Recall:", recall2)
  println("F1 score:", f1score2)


val printer = """# Podsumowanie
"""+"\n"+"""Metryki wbudowane:

"""+"Accuracy: "+accuracy+"\nF-score: "+fMeasure+"\nPrecision: "+precision+"\nRecall: "+recall
// PrintWriter
//Data.describe().rdd.map(_.toString().replace("[","").replace("]", "")).saveAsTextFile("frames")
import java.io._
val pw = new PrintWriter(new File("zapis.md"))
pw.println(printer)
pw.close


//Data.describe().write.format("csv").save("testing.csv")


 System.exit(0)
 
}

}

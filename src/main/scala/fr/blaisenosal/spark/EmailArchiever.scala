package fr.blaisenosal.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithSGD}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object EmailArchiever {

  // Création du Spark Context
  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("WordCount")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    // Nécessaire pour lire le "contenu" d'un email
    val hashing = new HashingTF()

    // Données d'entrainement
    val spam = sc.textFile("emails/spams/spam.txt").flatMap(mail => mail.split("\\s+"))
    val ham = sc.textFile("emails/spams/ham.txt").flatMap(mail => mail.split("\\s+"))

    val isSpam = new LabeledPoint(1, hashing.transform(spam.collect()))
    val isHam = new LabeledPoint(0, hashing.transform(ham.collect()))

    val trainingData = sc.parallelize(List(isSpam, isHam))
    val model = LogisticRegressionWithSGD.train(trainingData, 200)

    testModel(model, hashing)
  }

  def readSpam(folder: String): RDD[LabeledPoint] = {
    /* TODO Lire tous les fichiers ce dossier (+ récursivité si nécessaire)
     * Pour chaque fichier, créer un RDD[String] du mail correspondant et créer le LabeledPoint
     */
    null
  }

  def testModel(model: LogisticRegressionModel, hashing: HashingTF): Unit = {
    //tests
    val spamMail = hashing.transform(
      "insurance plan which change your life ...".split(" "))
    val nonSpam = hashing.transform(
      "hi sorry yaar i forget tell you i cant come today".split(" "))

    println("Prediction for positive test example: " + model.predict(spamMail))
    println("Prediction for negative test example: " + model.predict(nonSpam))
  }
}
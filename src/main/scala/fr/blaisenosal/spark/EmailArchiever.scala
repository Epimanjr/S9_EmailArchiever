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
    val spams = EmailReader.readEmails("emails/spams", sc, hashing, 1)
    val hams = EmailReader.readEmails("emails/hams", sc, hashing, 0)

    val trainingData = spams.union(hams)
    val model = LogisticRegressionWithSGD.train(trainingData, 200)

    testModel(model, hashing)
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
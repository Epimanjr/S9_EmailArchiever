package fr.blaisenosal.spark

import java.io.{BufferedWriter, FileWriter}

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithSGD}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object EmailArchiever {

  // Variables nécessaires
  val trainingDataFolder: String = "data_training"
  val toPredictDataFolder: String = "data_toanalyse"

  // Création du Spark Context
  val conf = new SparkConf()
    .setAppName("EmailArchiever")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    // Nécessaire pour lire le "contenu" d'un email
    val hashing = new HashingTF()

    // Données d'entrainement
    val spams = EmailReader.readEmails(s"${trainingDataFolder}/spams", sc, hashing, 1)
    val hams = EmailReader.readEmails(s"${trainingDataFolder}/hams", sc, hashing, 0)

    val trainingData = spams.union(hams)
    val model = LogisticRegressionWithSGD.train(trainingData, 200)

    //testModel(model, hashing)
    analyzeFolder(s"${toPredictDataFolder}", model, sc, hashing)
  }

  def analyzeFolder(path: String, model: LogisticRegressionModel, sc: SparkContext, hashing: HashingTF): Unit = {
    // Création d'un writer sur le fichier des résultats
    val writer = new BufferedWriter(new FileWriter(s"${path}/results.txt"))

    // Prédiction
    EmailReader.readEmailsToPredict(path, model, sc, hashing, writer)
    writer.close()
  }

  def testModel(model: LogisticRegressionModel, hashing: HashingTF): Unit = {
    //tests
    val spamMail = hashing.transform(
      "buy a SAMSUNG now for free !!".split(" "))
    val nonSpam = hashing.transform(
      "Hello I am your friend.".split(" "))

    println("Prediction for positive test example: " + model.predict(spamMail))
    println("Prediction for negative test example: " + model.predict(nonSpam))
  }
}
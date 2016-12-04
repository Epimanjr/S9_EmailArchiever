package fr.blaisenosal.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint

object SpamFilter {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("SpamFilter")
    val sc = new SparkContext(conf)
    val tf = new HashingTF()

    //récupère une base préexistante de spams et de non spams
    val spams = sc.textFile("files/mails/spam/spam.txt", 4)
    val ok = sc.textFile("files/mails/ham/ham.txt", 4)

    //on utilise un algorithme TF pour déterminer la fréquence des mots dans les spams
    val spamCaracs = spams.map(mail => tf.transform(mail.split("\\+s")))
    val okCaracs = spams.map(mail => tf.transform(mail.split("\\+s")))

    //on utilise des LabeledPoint pour indentifier les termes des spams et des non spams
    val isSpam = spamCaracs.map(feat => LabeledPoint(1, feat))
    val notSpam = okCaracs.map(feat => LabeledPoint(0, feat))

    isS
    val data = isSpam.union(notSpam)
    data.cache()

    //on utilise LogisticRegression car on veut prédire une réponse binaire : est un spam, n'est pas un spam
    val predictiveModel = new LogisticRegressionWithSGD().run(data)

    //tests
    val spamMail = tf.transform(
      "insurance plan which change your life ...".split(" "))
    val nonSpam = tf.transform(
      "hi sorry yaar i forget tell you i cant come today".split(" "))

    println("Prediction for positive test example: " + predictiveModel.predict(spamMail))
    println("Prediction for negative test example: " + predictiveModel.predict(nonSpam))

  }
}
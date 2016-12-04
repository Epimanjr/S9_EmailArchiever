package fr.blaisenosal.spark

import java.io.{BufferedWriter, File}

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector

/**
  * Created by Maxime BLAISE on 04/12/2016.
  */
object EmailReader {

  /**
    * Lecture d'un dossier d'emails pour le convertir en RDD de LabeledPoint
    *
    * @param path    Chemin du fichier contenant l'email
    * @param sc      SparkContext
    * @param hashing Hashing Transform
    * @param isSpam  1 si spam, 0 sinon
    * @return RDD de LabeledPoint
    */
  def readEmails(path: String, sc: SparkContext, hashing: HashingTF, isSpam: Int): RDD[LabeledPoint] = {
    /* TODO Lire tous les fichiers ce dossier (+ récursivité si nécessaire)
     * Pour chaque fichier, créer un RDD[String] du mail correspondant et créer le LabeledPoint
     */

    val folder = new File(path)
    var listLPoint: List[LabeledPoint] = List()

    // Lecture du contenu du dossier
    for (file <- folder.listFiles()) {
      if (file.isDirectory) {
        // TODO Récursivité
      } else {
        // Lecture de l'email
        val lPoint = readEmail(file.getAbsolutePath, sc, hashing, isSpam)


        // Ajout à la liste
        listLPoint = List.concat(listLPoint, List(lPoint))
      }
    }

    // Création RDD à partir de la liste
    val rddLPoint = sc.parallelize(listLPoint)
    rddLPoint
  }

  /**
    * Lecture d'un email pour le convertir en LabeledPoint
    *
    * @param path    Chemin du fichier contenant l'email
    * @param sc      SparkContext
    * @param hashing Hashing Transform
    * @param isSpam  1 si spam, 0 sinon
    * @return LabeledPoint
    */
  def readEmail(path: String, sc: SparkContext, hashing: HashingTF, isSpam: Int): LabeledPoint = {
    // Lecture du RDD de String
    val emailFlatMap = sc.textFile(path)
      .flatMap(mail => mail.split("\\s+"))

    // Conversion en LabeledPoint
    val lPoint = new LabeledPoint(isSpam, hashing.transform(emailFlatMap.collect()))
    lPoint
  }

  /**
    * Permet de lire tous les mails à prédire.
    *
    * @param path    Chemin du fichier contenant l'email
    * @param sc      SparkContext
    * @param hashing Hashing Transform
    * @return RDD de Vector
    */
  def readEmailsToPredict(path: String, model: LogisticRegressionModel, sc: SparkContext, hashing: HashingTF, writer: BufferedWriter): Unit = {
    val folder = new File(path)

    // Lecture du contenu du dossier
    for (file <- folder.listFiles()) {
      if (file.isDirectory) {
        // TODO Récursivité
      } else if (!file.getName.equals("results.txt")){
        // Lecture de l'email
        val vector = readEmailToPredict(file.getAbsolutePath, sc, hashing)

        val prediction = model.predict(vector)
        writer.append(s"Prediction for ${file.getName} -> ${prediction}\n")
      }
    }

  }

  /**
    * Permet de convertir le contenu d'un fichier entré en paramère en Vector
    * @param path    Chemin du fichier contenant l'email
    * @param sc      SparkContext
    * @param hashing Hashing Transform
    * @return Vector instance
    */
  def readEmailToPredict(path: String, sc: SparkContext, hashing: HashingTF): Vector = {
    val dataFile = sc.textFile(path)
            .flatMap(line => line.split("\\s+"))

    val tf = hashing.transform(dataFile.collect())
    tf
  }
}

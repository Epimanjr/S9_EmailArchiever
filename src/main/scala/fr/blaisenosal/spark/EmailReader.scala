package fr.blaisenosal.spark

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * Created by Maxime BLAISE on 04/12/2016.
  */
class EmailReader {

  def readSpam(path: String): RDD[LabeledPoint] = {
    /* TODO Lire tous les fichiers ce dossier (+ récursivité si nécessaire)
     * Pour chaque fichier, créer un RDD[String] du mail correspondant et créer le LabeledPoint
     */

    null
  }
}

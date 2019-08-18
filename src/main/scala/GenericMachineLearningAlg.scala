import org.apache.spark.sql.types.StringType
import org.apache.spark.{SparkConf, SparkContext}


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.types.StringType

import scala.collection.mutable

object GenericMachineLearningAlg extends SparkSessionBase {

  def machineLearningEx(): Unit = {


    /** Importer les données */
    val path = ""
    val Database = sparkSession.read.format("csv").option("header","true").option("inferschema","true").load(path)

    println("----------------------------")
    println("DATA")
    println("----------------------------")

    Database.show(8000)



    /** Lister les colonnes de features en fonction du type => Séparer les colonnes "numérique" et "non numériques" */
    val NoNumFeaturesColNames = mutable.ArrayBuffer[String]() // Noms de colonnes non numériques
    val NumFeaturesColNames = mutable.ArrayBuffer[String]() // Noms de colonnes numériques
    Database.schema.map(col => {
      if(col.dataType == StringType){
        NoNumFeaturesColNames += (col.name.toString)
      } else {
        NumFeaturesColNames += (col.name.toString)
      }
    })



    /** Creation de noms de colonnes :
      * Nom de colonne initiale = ColName
      * Nom de colonne après préprocessing = ColName_Indexed*/



    /** Colonne des labels */
    val labelColName = ""
    val idxdLabelColName = labelColName + "Indexed"

    /** colonnes features*/
    val idxdNoNumFeaturesColNames = NoNumFeaturesColNames.map(_ + "Indexed")
    val allFeaturesColNamesInterm = NumFeaturesColNames ++ NoNumFeaturesColNames
    val allIdxdFeaturesColNamesIntermediate = NumFeaturesColNames ++ idxdNoNumFeaturesColNames
    val features = "Features"

    val allFeaturesColNames = allFeaturesColNamesInterm.filter(_ != labelColName)
    val allIdxdFeaturesColNames = allIdxdFeaturesColNamesIntermediate.filter(_ != idxdLabelColName)

    /** Train test split */
    val splits = Database.randomSplit(Array(0.9,0.1))
    val (trainingData, testData) = (splits(0),splits(1))
    val allTrainingData = trainingData.select(labelColName, allFeaturesColNames: _*)
    val allTestData = testData.select(labelColName, allFeaturesColNames: _*)
    val allData = allTrainingData.union(allTestData)


    /** Encoder de String vers Int les features*/
    val stringIndexers = NoNumFeaturesColNames.map{colName =>
      new StringIndexer()
        .setInputCol(colName)
        .setOutputCol(colName + "Indexed")
        .fit(allData)
    }

    /** Encoder de String vers Int les labels*/
    val labelIndexer = new StringIndexer()
      .setInputCol(labelColName)
      .setOutputCol(idxdLabelColName)
      .fit(allData)

    /** Vectoriser les features*/
    val assembler = new VectorAssembler()
      .setInputCols(Array(allIdxdFeaturesColNames: _*))
      .setOutputCol(features)

    /** Random forest */
    val randomForest = new RandomForestClassifier()
      .setLabelCol(idxdLabelColName)
      .setFeaturesCol(features)

    /** Reconvertir les labels Int vers String */
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictionLabel")
      .setLabels(labelIndexer.labels)

    /** Définir la pipeline */
    val pipeline = new Pipeline().setStages(
      (stringIndexers  :+ assembler :+ randomForest :+ labelConverter).toArray)

    /** Paramêtres de validation croisée */
    val paramGrid = new ParamGridBuilder()
      .addGrid(randomForest.maxBins, Array(25,28,31))
      .addGrid(randomForest.maxDepth, Array(4,6,8))
      .addGrid(randomForest.impurity, Array("entropy","gini"))
      .build()

    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol(idxdLabelColName)

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)

    /** Entrainement du model */
    val crossValidatorModel = cv.fit(allTrainingData)


    println("----------------------------")
    println("PREDICTION")
    println("----------------------------")

    /** Prediction sur set de test */
    val predictions = crossValidatorModel.transform(allTestData)
    predictions.show(5000)

  }
}

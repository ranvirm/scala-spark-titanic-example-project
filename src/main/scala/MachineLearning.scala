import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{GBTClassifier, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.DataFrame

// do final data preparations for machine learning here
// define and run machine learning models here
// this should generally return trained machine learning models and or labelled data
// NOTE this may be merged with feature engineering to create a single pipeline

object MachineLearning {

  def pipelineFit(dataFrame: DataFrame): PipelineModel = {

    // define feature vector assembler
    val featureAssembler = new VectorAssembler()
      .setInputCols(Array[String](
          "Male",
          "Embarked_Indexed_Vec",
          "Pclass_Indexed_Vec",
          "Title_Vec",
          "FamilySize_Vec",
          "FareGroup_Vec",
          "AgeGroup_Vec"
        )
      )
      .setOutputCol("features")

    // define random forest estimator
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(10)
      .setMaxDepth(10)
      .setSeed(1L)

    // define gbt estimator
    val gbt = new GBTClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(100)
      .setSeed(1L)


    // chain indexers and forest in a Pipeline
    val pipeline = new Pipeline()
      .setStages(
        Array(
          featureAssembler,
          rf
        )
      )

    // fit pipeline
    val pipelineModel = pipeline.fit(dataFrame)

    // make predictions
    val predictions = pipelineModel.transform(dataFrame)

    // select (prediction, true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))

    // return fitted pipeline
    pipelineModel

  }

}

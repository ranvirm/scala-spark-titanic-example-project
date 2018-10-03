import org.apache.spark.ml.feature.{Bucketizer, OneHotEncoderEstimator, StringIndexer}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{split, when}

// Do your feature engineering here

object FeatureEngineering {

  // function that returns a data frame with added  features
  def featureData(dataFrame: DataFrame): DataFrame = {

    // function to index embarked col
    def embarkedIndexer(dataFrame: DataFrame): DataFrame = {

      val indexer = new StringIndexer()
        .setInputCol("Embarked")
        .setOutputCol("Embarked_Indexed")

      val indexed = indexer.fit(dataFrame).transform(dataFrame)

      indexed

    }

    // function to index pclass col
    def pclassIndexer(dataFrame: DataFrame): DataFrame = {

      val indexer = new StringIndexer()
        .setInputCol("Pclass")
        .setOutputCol("Pclass_Indexed")

      val indexed = indexer.fit(dataFrame).transform(dataFrame)

      indexed

    }

    // function to create age buckets
    def ageBucketizer(dataFrame: DataFrame): DataFrame = {

      // define splits for age buckets
      val ageSplits = Array(0.0, 5.0, 18.0, 35.0, 60.0, 150.0)

      // define age new bucketizer function
      val ageBucketizer = new Bucketizer()
        .setInputCol("Age")
        .setOutputCol("AgeGroup")
        .setSplits(ageSplits)

      // add age buckets to input data frame
      ageBucketizer.transform(dataFrame).drop("Age")

    }

    // function to create fare buckets
    def fareBucketizer(dataFrame: DataFrame): DataFrame = {

      // define splits for fare buckets
      val fareSplits = Array(0.0, 10.0, 20.0, 30.0, 50.0, 100.0, 1000.0)

      // define age new bucketizer function
      val fareBucketizer = new Bucketizer()
        .setInputCol("Fare")
        .setOutputCol("FareGroup")
        .setSplits(fareSplits)

      // add fare buckets to input data frame
     fareBucketizer.transform(dataFrame).drop("Fare")

    }

    // function to convert sex to binary
    def sexBinerizer(dataFrame: DataFrame): DataFrame = {

      // add binary sex column
      val outputDataFrame = dataFrame.withColumn("Male", when(dataFrame("Sex").equalTo("male"), 1).otherwise(0))

      // return data frame with binary sex column and drop original sex column
      outputDataFrame.drop("Sex")

    }

    // function to create title field
    def titleCreator(dataFrame: DataFrame): DataFrame = {

      // extract title field from name column
      val outputData = dataFrame
        .withColumn("Title_String", split(split(dataFrame("Name"), ",")(1), "[.]")(0))

      val indexer = new StringIndexer()
        .setInputCol("Title_String")
        .setOutputCol("Title")

      val indexed = indexer.fit(outputData).transform(outputData)

      indexed.drop("Name")

    }

    // function to create family size field
    def familySizeCreator(dataFrame: DataFrame): DataFrame = {

      // create field indicating number of people in family
      val inputData = dataFrame.withColumn("FamilyMembers", dataFrame("Parch") + dataFrame("SibSp"))

      // create family size field by bucketing family size field
      val outputData = inputData.withColumn("FamilySize",
        when(inputData("FamilyMembers")===0, 1)
          .when(inputData("FamilyMembers")>0 && inputData("FamilyMembers")<4, 2)
          .otherwise(3))

      // return data frame with added family size field adn drop original columns used
      outputData.drop("FamilyMembers").drop("Parch").drop("SibSp")

    }

    // define function to create dummy variables from input columns
    // NOTE: this will be replaced with a one hot encoder
    def dummyCreator(dataFrame: DataFrame, dummyCols: Array[String]): DataFrame = {

      // create temp immutable data frame to be used in for loop
      var data = dataFrame

      // function to create binary columns for each unique value in input columns
      for(col <- dummyCols) {

        // create list of unique values in input column
        val uniqueValues = data.select(col).distinct().collect()

        // create binary column for each value in unique value list
        for(i <- uniqueValues.indices) {

          // define name for new column as the unique value
          var colName = uniqueValues(i).get(0)

          // remove special characters from name for new column
          colName = colName.toString.replaceAll("[.]", "_")

          // add binary column to mutable data frame
          data = data.withColumn(col+"_"+colName, when(data(col)===uniqueValues(i)(0), 1).otherwise(0))
        }

        // drop column that has been converted to binary
        data = data.drop(col)

      }

      // return transformed data frame
      data

    }

    // define cols to make dummy
    val dummyCols = Array[String]("Embarked", "Pclass", "Title", "FamilySize", "FareGroup", "AgeGroup")

    val oneHotEncoder = new OneHotEncoderEstimator()
      .setInputCols(Array[String]("Embarked_Indexed", "Pclass_Indexed", "Title", "FamilySize", "FareGroup", "AgeGroup"))
      .setOutputCols(Array[String]("Embarked_Indexed_Vec", "Pclass_Indexed_Vec", "Title_Vec", "FamilySize_Vec", "FareGroup_Vec", "AgeGroup_Vec"))


    // create output data frame by transforming inout data frame with each function defined above
    // NOTE: this will be replaced with a pipeline
    // SOON!
    val oneHotModel = oneHotEncoder.fit(
      pclassIndexer(
        embarkedIndexer(
          familySizeCreator(
            titleCreator(
              ageBucketizer(
                fareBucketizer(
                  sexBinerizer(dataFrame=dataFrame
                  )
                )
              )
            )
          )
        )
      )
    )

    val outputData = oneHotModel.transform(
      pclassIndexer(
        embarkedIndexer(
          familySizeCreator(
            titleCreator(
              ageBucketizer(
                fareBucketizer(
                  sexBinerizer(dataFrame=dataFrame
                  )
                )
              )
            )
          )
        )
      )
    )

    // return data frame with added features
    outputData

  }

}

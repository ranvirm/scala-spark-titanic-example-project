import DataCleaner.CleanData
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{split, when}

// Do your feature engineering here

object FeatureEngineering {

  // function that returns a data frame with added  features
  def FeatureData(): DataFrame = {

    // function to create age buckets
    def AgeBucketizer(dataFrame: DataFrame): DataFrame = {

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
    def FareBucketizer(dataFrame: DataFrame): DataFrame = {

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
    def SexBinerizer(dataFrame: DataFrame): DataFrame = {

      // add binary sex column
      val outputDataFrame = dataFrame.withColumn("Male", when(dataFrame("Sex").equalTo("male"), 1).otherwise(0))

      // return data frame with binary sex column and drop original sex column
      outputDataFrame.drop("Sex")

    }

    // function to create title field
    def TitleCreator(dataFrame: DataFrame): DataFrame = {

      // extract title field from name column
      val outputData = dataFrame
        .withColumn("Title", split(split(dataFrame("Name"), ",")(1), "[.]")(0))

      outputData.drop("Name")

    }

    // function to create family size field
    def FamilySizeCreator(dataFrame: DataFrame): DataFrame = {

      // create field indicating number of people in family
      val inputData = dataFrame.withColumn("FamilyMembers", dataFrame("Parch") + dataFrame("SibSp"))

      // create family size field by bucketing family size field
      val outputData = inputData.withColumn("FamilySize",
        when(inputData("FamilyMembers")===0, "None")
          .when(inputData("FamilyMembers")>0 && inputData("FamilyMembers")<4, "Small")
          .otherwise("Large"))

      // return data frame with added family size field adn drop original columns used
      outputData.drop("FamilyMembers").drop("Parch").drop("SibSp")

    }

    // define function to create dummy variables from input columns
    def DummyCreator(dataFrame: DataFrame, dummyCols: Array[String]): DataFrame = {

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

    // create output data frame by transforming inout data frame with each function defined above
    val outputData = DummyCreator(FamilySizeCreator(TitleCreator(AgeBucketizer(FareBucketizer(SexBinerizer(CleanData()))))), dummyCols)

    // return data frame with added features
    outputData

  }

}

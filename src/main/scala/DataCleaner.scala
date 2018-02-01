import DataSourcer.RawData
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{mean, round}

// Clean raw data frames here - format columns, replace missing values etc.

object DataCleaner {

  // function to produce a clean data frame from a raw data frame
  def CleanData(): DataFrame = {

    // def function to format data correctly
    def formatData(dataFrame: DataFrame): DataFrame = {

      dataFrame
        .withColumn("Age", dataFrame("Age").cast("Double"))
        .withColumn("Fare", dataFrame("Fare").cast("Double"))
        .withColumn("Parch", dataFrame("Parch").cast("Double"))
        .withColumn("SibSp", dataFrame("SibSp").cast("Double"))

    }

    // function to calculate mean age from input data frame. Returns a value of type double
    def meanAge(dataFrame: DataFrame): Double = {

      // select age column and calulate mean age from non-NA values
      dataFrame
        .select("Age")
        .na.drop()
        .agg(round(mean("Age"), 0))
        .first()
        .getDouble(0)

    }

    // function to calculate mean fare from input data frame. Returns a value of type double
    def meanFare(dataFrame: DataFrame): Double = {

      // select fare column and calculate mean fare from non-NA values
      dataFrame
        .select("Fare")
        .na.drop()
        .agg(round(mean("Fare"), 2))
        .first()
        .getDouble(0)

    }

    // format raw data
    val formattedData = formatData(RawData())

    // fill in missing values for age, fare and embarked and drop unnecessary columns
    val outputData = formattedData
      .na.fill(Map(
      "Age" -> meanAge(formattedData),
      "Fare" -> meanFare(formattedData),
      "Embarked" -> "S"
      ))
      .drop("Ticket", "Cabin")

    // return cleaned data frame
    outputData

  }


}

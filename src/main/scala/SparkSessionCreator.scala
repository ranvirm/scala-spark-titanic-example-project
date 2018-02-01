import org.apache.spark.sql.SparkSession

// Create or retrieve a spark session here
// Change the master to 'yarn' when running on the Asgard cluster

object SparkSessionCreator {

  def Spark(): SparkSession = {

    SparkSession.builder().master("local").appName("ScalaProjectExample").getOrCreate()

  }

}

import org.apache.spark.sql.SparkSession

object PostgresReadHiveWriteIncremental {

  def main(args: Array[String]): Unit = {

    // Initialize a Spark session
    val spark = SparkSession.builder
      .appName("PostgresToHive")
      .enableHiveSupport()
      .getOrCreate()

    // Hive table
    val hiveDatabase = "weather_forecast"
    val hiveTableName = "emp"
    val hiveTableFullName = s"$hiveDatabase.$hiveTableName"

    // Find the max(emp_id) in the Hive table
    val hiveMaxId = spark.sql(s"SELECT MAX(emp_id) as max_id FROM $hiveTableFullName").collect()(0).getAs[Long]("max_id")

    // PostgreSQL database URL
    val postgresDbUrl = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"

    // Define PostgreSQL query to get rows with emp_id > hiveMaxId
    val postgresQuery = s"(SELECT * FROM emp WHERE emp_id > $hiveMaxId) AS E1"

    // Read data from PostgreSQL to Spark DataFrame
    val dfPostgres = spark.read.format("jdbc")
      .option("url", postgresDbUrl)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", postgresQuery)
      .option("user", "consultants")
      .option("password", "WelcomeItc@2022")
      .load()

    // Show DataFrame content
    dfPostgres.show()

    // Check if there are extra rows in PostgreSQL
    if (dfPostgres.count() > 0) {
      // Append new rows to Hive table
      dfPostgres.write.mode("append").saveAsTable(hiveTableFullName)
      println(s"Appended ${dfPostgres.count()} new records to Hive table.")
    } else {
      println("No new rows in PostgreSQL table.")
    }

    // Stop the Spark session
    spark.stop()
  }
}

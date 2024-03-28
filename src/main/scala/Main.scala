package ProjectBruno

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{asc, avg, col, collect_set, desc, isnan, lit, regexp_replace, to_date, when}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType}

object Main {
  def main(args: Array[String]): Unit = {
   // System.setProperty("hadoop.home.dir","C:/Handoop/")

   // Abrir a App Spark
    val spark = SparkSession.builder()
      .appName("XpandIt")
      .master("local[*]")
      .config("spark.driver.bidAddress", "127.0.0.1")
      .getOrCreate()

 /* ++++++++++++++++    Abrir o CSV User Reviews +++++++++++++++++ */

   // CSV Google Store
    val dfReview = spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("Data/googleplaystore_user_reviews.csv")

   // dfReview.show(50,truncate = false) // Estava desformatado o dataframe e meti um truncate a true
   // dfReview.printSchema()

    /* ++++++++++++++++ Parte 1 +++++++++++++++++ */
    // Criei um Dataframe que passa a Col SP para double e passa os null para 0
    val df_1 = dfReview.select(
        col("App"),
        col("Sentiment_Polarity").cast("double"))
            .na.fill(0, Seq("Sentiment_Polarity"))
            .groupBy("App")
            .agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))
            .orderBy(asc("App")
    )

    df_1.show(50, truncate = false)
    df_1.printSchema()




    /* ++++++++++++++++ Parte 2 +++++++++++++++++ */
    val dfstore = spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("Data/googleplaystore.csv")

    // Filtrar todas as Apps com "Rating" maior ou igual a 4.0 e que não sejam NaN
    val bestApps = dfstore.filter(!(isnan(col("Rating"))) && col("Rating") >= 4.0)
      .orderBy(desc("Rating"))

    bestApps.show()
    // Salvar o DataFrame filtrado como um único arquivo CSV

    /*  bestApps
      .write
      .option("header", "true")
      .option("delimiter", "§")
      .csv("Data/best_apps")  // Não percebo, ele cria um Diretorio e não um ficheiro CSV */



    /* usei isto para saber quantas linhas foram alteradas.
    val contar1  = dfstore.count()
    println(s"Linhas Antes: $contar1")
    val contar  = bestApps.count()
    println(s"Linhas Atuais: $contar")
    */


    /* ++++++++++++++++ Parte 3 +++++++++++++++++ */

    // Mudar o data type

    val dfDatatype = dfstore.select(
      col("App").cast(StringType),
      col("Category").cast(StringType),
      col("Reviews").cast(LongType),
      col("Rating").cast(DoubleType),
      col("Size").cast(DoubleType),
      col("Installs").cast(StringType),
      col("Type").cast(StringType),
      col("Price").cast(DoubleType),
      col("Content Rating").cast(StringType),
      col("Genres").cast(StringType),
      to_date(col("Last Updated"), "yyyy-MM-dd").alias("Last Updated"),
      col("Current Ver").cast(StringType),
      col("Android Ver").cast(StringType)
    )


    val dfProcessed = dfDatatype
      .groupBy("App")
      .agg(
        collect_set("Category").as("Categories"),
        functions.max("Reviews").as("Reviews"),
        functions.max("Size").as("Size"),
        functions.max("Installs").as("Installs"),
        functions.max("Type").as("Type"),
        functions.max("Price").as("Price"),
        functions.max("Content Rating").as("ContentRating"),
        functions.max("Genres").as("Genres"),
        to_date(functions.max("Last Updated"), "yyyy-MM-dd").as("LastUpdated"),
        functions.max("Current Ver").as("CurrentVersion"),
        functions.max("Android Ver").as("MinimumAndroidVersion")
      )
      .withColumn("Size",
        when(col("Size").endsWith("M"), regexp_replace(col("Size"), "M", "").cast(DoubleType) * 1024)
          .otherwise(regexp_replace(col("Size"), "k", "").cast(DoubleType))
      )
      .withColumn("Price", col("Price") * 0.9) // Conversão da moeda para euros
      .withColumn("Genres", functions.split(col("Genres"), ";")) //  array strings com ,
      .orderBy("App")

    dfProcessed.show()







  }


}
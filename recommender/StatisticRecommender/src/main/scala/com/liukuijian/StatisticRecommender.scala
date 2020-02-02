import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


case class Movie(mid:Int, name:String, descri:String,
                 timelong:String, issue:String, shoot:String,
                 language:String, genres:String, actors:String, directors:String)

case class Rating(uid:Int, mid: Int, score:Double, timestamp: Long)

case class Tag(uid:Int, mid:Int, tag:String, timestamp:Long)

//把MongoDB和ES的配置封装
case class MongoConfig(uri: String, db:String)

case class Recommendation(mid:Int, score:Double)

case class GenresRecommendation(genres:String,recs:Seq[Recommendation])


object StatisticRecommender {

    val MONGODB_MOVIE_COLLECTION = "Movie"
    val MONGODB_RATING_COLLECTION = "Rating"
    val MONGODB_TAG_COLLECTION = "Tag"

    //统计表的名称
    val RATE_MORE_MOVIES = "RateMoreMovies"
    val RATE_MORE_RECNETLY_MOVIES = "RateMoreRecentlyMovies"
    val AVERAGE_MOVIES = "AverageMovies"
    val GENRES_TOP_MOVIES = "GenresTopMovies"

    def main(args: Array[String]): Unit = {
        //创建一个sparkconfig
        val config = Map(
            "spark.core" -> "local[*]",
            "mongo.uri" -> "mongodb://hadoop101:27017/recommender",
            "mongo.db" -> "recommender"
        )
        val conf: SparkConf = new SparkConf().setMaster(config("spark.core")).setAppName("StatisticRecommender")

        val spark = SparkSession.builder().config(conf).getOrCreate()

        import spark.implicits._

        implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

        //将数据读进来
        val movieDF: DataFrame =
            spark
                    .read
                    .option("uri", mongoConfig.uri)
                    .option("collection", MONGODB_MOVIE_COLLECTION)
                    .format("com.mongodb.spark.sql")
                    .load()
                    .as[Movie]
                    .toDF

        val ratingDF: DataFrame =
            spark
                    .read
                    .option("uri", mongoConfig.uri)
                    .option("collection", MONGODB_RATING_COLLECTION)
                    .format("com.mongodb.spark.sql")
                    .load()
                    .as[Rating]
                    .toDF

        ratingDF.createOrReplaceTempView("ratings")

        //需求1：每部电影的点击次数越多就越热门，历史热门电影统计
        val rateMoreMovieDF: DataFrame =
            spark.sql(
                """
                  |select mid, count(mid) as count
                  |from ratings
                  |group by mid
                """.stripMargin)

        storeDFInMongoDB(rateMoreMovieDF, RATE_MORE_MOVIES)

        //需求2：最近热门电影统计
        //根据评分按月为单位计算最近时间的月份里面评分次数最多的电影集合
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMM")

        spark.udf.register("changeDate", (x:Long) => sdf.format(new Date(x*1000L)).toInt)

        var ratingOfYearMonth: DataFrame = spark.sql(
            """
              |select mid, changeDate(timestamp) as month, score
              |from ratings
              |
            """.stripMargin)
        ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")

        var rateMoreRecentlyMovies: DataFrame = spark.sql(
            """
              |select mid, count(mid) as count, month
              |from ratingOfMonth
              |group by month, mid
            """.stripMargin)

        storeDFInMongoDB(rateMoreRecentlyMovies, RATE_MORE_RECNETLY_MOVIES)


        //需求3：电影平均得分统计
        val averageMovies: DataFrame =spark.sql(
            """
              |select mid, avg(score) as avg
              |from ratings
              |group by mid
            """.stripMargin)

        storeDFInMongoDB(averageMovies, AVERAGE_MOVIES)

        //需求4： 每个类别优质电影统计
        //根据提供的所有电影类别，分别计算每种类型的电影集合中评分最高的10个电影

        val movieWithScore: DataFrame = movieDF.join(averageMovies, Seq("mid"))//按mid做内连接，得到每部电影的平均得分

        val genres = List("Action","Adventure","Animation","Comedy",
        "Crime","Documentary","Drama","Family","Fantasy","Foreign",
        "History","Horror","Music","Mystery" ,"Romance","Science",
        "Tv","Thriller","War","Western")

        val genresRDD: RDD[String] = spark.sparkContext.makeRDD(genres)

        val genresTopMovies: DataFrame =
                    genresRDD
                            .cartesian(movieWithScore.rdd)
                            .filter{
                                case (genres, row: Row) =>
                                    row.getAs[String]("genres").toLowerCase.contains(genres.toLowerCase)
                            }
                            .map{
                                case(genres, movieRow) =>
                                    (genres,(movieRow.getAs[Int]("mid"),movieRow.getAs[Double]("avg")))
                            }
                            .groupByKey
                            .map{
                                case (genres, items) =>
                                    GenresRecommendation(genres, items.toList.sortBy(-_._2).take(10).map(
                                        item =>Recommendation(item._1,item._2)
                                    ))
                            }
                            .toDF()
        storeDFInMongoDB(genresTopMovies, GENRES_TOP_MOVIES)
        spark.stop()
    }

    def storeDFInMongoDB(df:DataFrame, collection_name:String)(implicit mongoConfig: MongoConfig): Unit ={
        df.write
          .option("uri",mongoConfig.uri)
          .option("collection",collection_name)
          .mode("overwrite")
          .format("com.mongodb.spark.sql")
          .save()
    }

}

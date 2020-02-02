package com.liukuijian.recommender

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


case class Movie(mid:Int, name:String, descri:String,
                 timelong:String, issue:String, shoot:String,
                 language:String, genres:String, actors:String, directors:String)

case class Rating(uid:Int, mid: Int,score:Double , timestamp: Long)

case class Tag(uid:Int, mid:Int, tag:String,timestamp:Long)

//把MongoDB和ES的配置封装
case class MongoConfig(uri: String, db:String)

case class ESConfig(httpHosts:String, transportHosts:String, index:String, clustername:String)

//由于不熟悉es的具体用法，先不管涉及到es的模块
object DataLoader {

    //定义常量
    val MOVIE_DATA_PATH = "E:\\IdeaProjects\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\movies.csv"
    val RATING_DATA_PATH = "E:\\IdeaProjects\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
    val TAG_DATA_PATH = "E:\\IdeaProjects\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\tags.csv"

    val MONGODB_MOVIE_COLLECTION = "Movie"
    val MONGODB_RATING_COLLECTION = "Rating"
    val MONGODB_TAG_COLLECTION = "Tag"
    val ES_MOVIE_INDEX = "Movie"

    def main(args: Array[String]): Unit = {
        //创建一个sparkconfig
        val config = Map(
            "spark.core" -> "local[*]",
            "mongo.uri" -> "mongodb://hadoop101:27017/recommender",
            "mongo.db" -> "recommender",
            "es.httpHosts" -> "localhost:9200",
            "es.transportHosts" ->  "localhost:9300",
            "es.index" -> "recommender",
            "es.cluster.name" -> "elasticsearch"
        )
        val conf: SparkConf = new SparkConf().setMaster(config("spark.core")).setAppName("DataLoader")

        val spark = SparkSession.builder().config(conf).getOrCreate()

        import spark.implicits._

        val movieRDD: RDD[String] = spark.sparkContext.textFile(MOVIE_DATA_PATH)
        val movieDF: DataFrame = movieRDD.map(
            item => {
                val attr: Array[String] = item.split("\\^")
                Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim, attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
            }
        ).toDF()

        val ratingRDD: RDD[String] = spark.sparkContext.textFile(RATING_DATA_PATH)
        val ratingDF: DataFrame = ratingRDD.map(
            item => {
                val attr: Array[String] = item.split(",")
                Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toLong)
            }
        ).toDF

        val tagRDD: RDD[String] = spark.sparkContext.textFile(TAG_DATA_PATH)
        val tagDF: DataFrame = tagRDD.map(
            item => {
                val attr: Array[String] = item.split(",")
                Tag(attr(0).toInt, attr(1).toInt, attr(2), attr(3).toLong)
            }
        ).toDF
        //mongdb config
        implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

        storeDataInMongoDB(movieDF, ratingDF, tagDF)

        spark.stop()
    }

    def storeDataInMongoDB(movieDF:DataFrame, ratingDF:DataFrame, tagDF:DataFrame)(implicit  mongoConfig: MongoConfig): Unit ={
        //新建一个mongdb的连接
        val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

        //如果MongoDB中有对应的数据库，那么应该删除
        mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
        mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()
        movieDF
                .write
                .option("uri",mongoConfig.uri)
                .option("collection",MONGODB_MOVIE_COLLECTION)
                .mode("overwrite")
                .format("com.mongodb.spark.sql")
                .save()
        ratingDF
                .write
                .option("uri",mongoConfig.uri)
                .option("collection",MONGODB_RATING_COLLECTION)
                .mode("overwrite")
                .format("com.mongodb.spark.sql")
                .save()
        tagDF
                .write
                .option("uri",mongoConfig.uri)
                .option("collection",MONGODB_TAG_COLLECTION)
                .mode("overwrite")
                .format("com.mongodb.spark.sql")
                .save()
        //对数据表建索引
        mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

        mongoClient.close()
    }
}

package com.liukuijian

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix


case class MovieRating(uid:Int,mid:Int, score:Double, timestamp:Long)

case class Recommendation(mid:Int, score:Double)

case class MongoConfig(uri: String, db:String)

case class UserRecs(uid:Int,recs:Seq[Recommendation])

case class MovieRecs(mid:Int,recs:Seq[Recommendation])

object OfflineRecommender {

    val MONGODB_RATING_COLLECTION = "Rating"

    val USER_RECS = "User_Recs"

    val MOVIE_RECS = "Movie_Recs"

    val USER_MAX_RECOMMENDATION = 20

    def main(args: Array[String]): Unit = {

        val config = Map(
            "spark.core" -> "local[*]",
            "mongo.uri" -> "mongodb://hadoop101:27017/recommender",
            "mongo.db" -> "recommender"
        )
        val conf: SparkConf = new SparkConf().setMaster(config("spark.core")).setAppName("OfflineRecommender")

        val spark = SparkSession.builder().config(conf).getOrCreate()

        import spark.implicits._

        implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

        val ratingRDD: RDD[(Int, Int, Double)] =
                        spark
                            .read
                            .option("uri", mongoConfig.uri)
                            .option("collection", MONGODB_RATING_COLLECTION)
                            .format("com.mongodb.spark.sql")
                            .load()
                            .as[MovieRating]
                            .rdd
                            .map(rating => (rating.uid,rating.mid,rating.score))
                            .cache()
        val userRDD: RDD[Int] = ratingRDD.map(_._1).distinct()
        val movieRDD: RDD[Int] = ratingRDD.map(_._2).distinct()

        //训练隐语义模型LFM
        val trainData: RDD[Rating] = ratingRDD.map(x => Rating(x._1, x._2, x._3))

        val (rank, iteration, lambda) = (200,5,0.1)
        val model: MatrixFactorizationModel = ALS.train(trainData, rank, iteration, lambda)

        //计算user和movie
        val userMovies: RDD[(Int, Int)] = userRDD.cartesian(movieRDD)
        //基于用户和电影的隐特征，计算预测评分，得到用户的推荐列表
        val preRatings: RDD[Rating] = model.predict(userMovies)


        //评分大于0才有效
        val userRecs: DataFrame = preRatings.filter(_.rating > 0)
                //计算每个用户对每部电影的评分预测矩阵
                .map {
                    rating => (rating.user, (rating.product, rating.rating))
                }
                .groupByKey
                //得到所有预测评分，按分数高低排序，取前20
                .map {
                    case (uid, recs) =>
                        (uid, recs
                                .toList
                                .sortBy(-_._2)
                                .take(USER_MAX_RECOMMENDATION)
                                .map(rating => Recommendation(rating._1, rating._2)))
                }
                .toDF

        userRecs.write
                .option("uri",mongoConfig.uri)
                .option("collection",USER_RECS)
                .mode("overwrite")
                .format("com.mongodb.spark.sql")
                .save

        //基于电影隐特征，计算相似度矩阵，得到电影的相似度列表
        val movieFeatures: RDD[(Int, DoubleMatrix)] = model.productFeatures.map {
            case (mid, features) => (mid, new DoubleMatrix(features))
        }
        //对所有电影俩俩计算他们的相似度，先做笛卡尔积
        val movieRecs: RDD[MovieRecs] =
            movieFeatures.cartesian(movieFeatures)
                .filter{
                    //把自己跟自己的配对过滤掉
                    case (a, b) => a._1 != b._1
                }
                .map{
                    case (a,b) =>{
                        val simScore: Double = this.consinSim(a._2,b._2)
                        (a._1,(b._1,simScore))
                    }
                }
                .filter(_._2._2 > 0.6) //过滤出相似度大于0.6
                .groupByKey
                .map{
                    case (mid, items) =>
                        MovieRecs(mid, items.toList.sortBy(-_._2).map(x => Recommendation(x._1,x._2)))
                }
        movieRecs.toDF
                .write
                .option("uri",mongoConfig.uri)
                .option("collection",MOVIE_RECS)
                .mode("overwrite")
                .format("com.mongodb.spark.sql")
                .save
    }

    //求向量余弦相似度
    def consinSim(matrix: DoubleMatrix, matrix1: DoubleMatrix): Double ={
        matrix.dot(matrix1) / (matrix.norm2() * matrix1.norm2())
    }
}

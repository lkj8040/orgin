package com.liukuijian

import breeze.numerics.sqrt
import com.liukuijian.OfflineRecommender.MONGODB_RATING_COLLECTION
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ALSTrainer {
    def main(args: Array[String]): Unit = {
        val config = Map(
            "spark.core" -> "local[*]",
            "mongo.uri" -> "mongodb://hadoop101:27017/recommender",
            "mongo.db" -> "recommender"
        )
        val conf: SparkConf = new SparkConf().setMaster(config("spark.core")).setAppName("ALSTrainer")

        val spark = SparkSession.builder().config(conf).getOrCreate()

        import spark.implicits._

        implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

        val ratingRDD: RDD[Rating] =
            spark.read
                .option("uri", mongoConfig.uri)
                .option("collection", MONGODB_RATING_COLLECTION)
                .format("com.mongodb.spark.sql")
                .load()
                .as[MovieRating]
                .rdd
                .map(rating => Rating(rating.uid,rating.mid,rating.score))
                .cache()
        //随机切分数据集，生成训练集和测试集
        val splits: Array[RDD[Rating]] = ratingRDD.randomSplit(Array(0.8,0.2))
        val trainingRDD: RDD[Rating] = splits(0)
        val testRDD: RDD[Rating] = splits(1)

        //模型参数选择
        adjustASLParam(trainingRDD, testRDD)

        spark.close()
    }

    def adjustASLParam(trainingRDD: RDD[Rating], testRDD: RDD[Rating]): Unit ={
        val result: Array[(Int, Double, Double)] =
            for(rank  <- Array(20,50,100,200,300); lambda <- Array(0.001, 0.01, 0.1))
                yield{
                    val model = ALS.train(trainingRDD,rank,5, lambda)
                    val rmse = getRMSE(model, testRDD)
                    (rank, lambda, rmse)
                }
        //控制台打印输出 最优参数
        println(result.minBy(_._3))
    }
    def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]) ={
        // 计算预测评分
        val userProducts: RDD[(Int, Int)] = data.map(item =>(item.user, item.product))
        val predictRating: RDD[Rating] = model.predict(userProducts)

        //以uid和mid作为外键，建立内连接
        val observed: RDD[((Int, Int), Double)] = data.map(item => ((item.user, item.product),item.rating))
        val predict: RDD[((Int, Int), Double)] = predictRating.map(item => ((item.user, item.product),item.rating))
        //内连接得到（uid, mid）,(actual, predict)
        val quart: Double =
            observed.join(predict)
                    .map {
                        case ((uid, mid), (actual, pre)) =>
                            val err = actual - pre
                            err * err
                    }
                    .mean()
        sqrt(quart)
    }
}

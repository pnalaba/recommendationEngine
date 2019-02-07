To compile -
sbt compile
sbt package


To run spark application -
spark-submit --master yarn --class RecommendationEngine target/scala-2.11/recommendationengine-2.11-1.0.jar build.sbt

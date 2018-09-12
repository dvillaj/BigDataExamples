# pyspark  --packages mysql:mysql-connector-java:5.1.26

df = sqlContext.read \
  .format("jdbc") \
  .option("url", "jdbc:mysql://localhost/movielens") \
  .option("driver", "com.mysql.jdbc.Driver") \
  .option("dbtable", "movierating") \
  .option("user", "root") \
  .option("password", "cloudera") \
  .load()

df.printSchema()

countsByUserID = df.groupBy("userid").count()
countsByUserID.show() 
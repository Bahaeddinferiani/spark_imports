def readreq(spark,user,password):
    readrequest = spark.read \
         .format("jdbc") \
         .option("driver","com.mysql.cj.jdbc.Driver") \
         .option("url", "jdbc:mysql://localhost:3306/employees") \
         .option("user", user) \
         .option("password", password) \
         .option("dbtable","salaries") \
         .option("lowerBound", 0)\
         .option("upperBound",440000)\
         .option("numPartitions", 6)\
         .option("partitionColumn", "emp_no")\
         .load()
    return readrequest
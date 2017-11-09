val json = spark.read.json("s3://verve-opsdata/data/rtb/rtbnobid/ds=2017-11-01/prod-rtb-log-00470a7f38824dd50*")
json.select($"dvc_uis",$"dvc_uid",$"geo_lat",$"geo_lon").map(_.mkString("\t")).write.text("/json-out")

json.repartition(640).write.option("compression","gzip").parquet("s3://verve-opsdata/temp/rtb-parquet2/")

//<restart spark shell>

val parquet = spark.read.parquet("s3://verve-opsdata/temp/rtb-parquet2/")
parquet.select($"dvc_uis",$"dvc_uid",$"geo_lat",$"geo_lon").map(_.mkString("\t")).write.text("/parquet-out")

// results on a 50 r3x.large cluster:
// reading json took 18 mins
// reading parquet took 1 min

// input sizes:
// json 547G
// parquet 271G

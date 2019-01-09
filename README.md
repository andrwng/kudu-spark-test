`spark-submit --master yarn --deploy-mode cluster --conf spark.yarn.appMasterEnv.HADOOP_USER_NAME=hdfs --class test.TestMain kudu-spark-test-1.0-SNAPSHOT-jar-with-dependencies.jar`

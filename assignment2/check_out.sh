echo ---- Deleting
rm hadoop_out/*
rm spark_out/*
rm normalized_outputA.txt
rm normalized_outputB.txt

echo ---- Fetching files from HDFS
hadoop fs -get /user/h45dong/a2_starter_code_output_spark/part-* ./spark_out/
hadoop fs -get /user/h45dong/a2_starter_code_output/part-* ./hadoop_out/
ls spark_out/
ls hadoop_out/

echo ---- Comparing
cat hadoop_out/part-* | sort > normalized_outputA.txt 
cat spark_out/part-* | sort > normalized_outputB.txt 
diff normalized_outputA.txt normalized_outputB.txt

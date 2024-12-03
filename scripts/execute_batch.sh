spark-submit-with-hive --executor-memory=4g --driver-memory=4g --class PopularityBasedRecs batch-layer/target/uber-batch-layer-1.0-SNAPSHOT.jar
beeline -u 'jdbc:hive2://10.0.0.50:10001/;transportMode=http' -f batch-layer/scripts/Populate_PopularRecs_HBase.hql

spark-submit-with-hive --executor-memory=4g --driver-memory=4g --class ContentBasedRecs batch-layer/target/uber-batch-layer-1.0-SNAPSHOT.jar
beeline -u 'jdbc:hive2://10.0.0.50:10001/;transportMode=http' -f batch-layer/scripts/Populate_ContentBasedRecs_HBase.hql

spark-submit-with-hive --executor-memory=4g --driver-memory=4g --class ALSBasedRecs batch-layer/target/uber-batch-layer-1.0-SNAPSHOT.jar
beeline -u 'jdbc:hive2://10.0.0.50:10001/;transportMode=http' -f batch-layer/scripts/Populate_ALSBasedRecs_HBase.hql
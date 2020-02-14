# spark-assignment

Arguments - inputEdrPath , output_db, output_table_name

/usr/hdp/2.6.5.0-292/spark2/bin/spark-submit --class assignment1.DomainHitsTonnage --files DomainHitsTonnage.json --properties-file spark-properties assignment-1.0-jar-with-dependencies.jar DomainHitsTonnage.json

/usr/hdp/2.6.5.0-292/spark2/bin/spark-submit --class assignment1.ContentTypeContribution --files ContentTypeContribution.json --properties-file spark-properties assignment-1.0-jar-with-dependencies.jar ContentTypeContribution.json

/usr/hdp/2.6.5.0-292/spark2/bin/spark-submit --class assignment1.Top5ContentTypes --files Top5ContentTypes.json --properties-file spark-properties assignment-1.0-jar-with-dependencies.jar Top5ContentTypes.json

/usr/hdp/2.6.5.0-292/spark2/bin/spark-submit --class assignment1.Top10Subscribers --files Top10Subscribers.json --properties-file spark-properties assignment-1.0-jar-with-dependencies.jar Top10Subscribers.json

/usr/hdp/2.6.5.0-292/spark2/bin/spark-submit --class assignment1.GGSNNameTonnage  --files GGSNNameTonnage.json --properties-file spark-properties assignment-1.0-jar-with-dependencies.jar GGSNNameTonnage.json


Understand:
Spark UI (for each job)


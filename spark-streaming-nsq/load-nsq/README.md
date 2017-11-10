# /srcDir contains a file with paths to json/gzip files

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -numReduceTasks 0 -file load-nsq.sh -mapper load-nsq.sh -input /srcDir -output /tgtDir

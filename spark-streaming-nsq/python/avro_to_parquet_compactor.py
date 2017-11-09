#
# Run with:
# spark-submit --packages com.databricks:spark-avro_2.11:3.2.0 avro-to-parquet-compactor.py --input-root-path input/ --output-root-path output/
#
import os
import re
import sys
import configargparse

def is_s3_path(path):
   return path.startswith('s3://') or path.startswith('s3a://') or path.startswith('s3n://')

def normalize_path(path):
   normalized_path = path
   if not is_s3_path(normalized_path):
      normalized_path = os.path.expanduser(normalized_path)
      normalized_path = os.path.abspath(normalized_path)
   return re.sub('/$', '', normalized_path)

def exec_script(cmd):
   with os.popen(cmd) as file:
      return file.read().strip()

def exec_script_lines(cmd):
   with os.popen(cmd) as file:
      return map(lambda line: line.strip(), file.readlines())

def last_completed_date(root_path):
   normalized_root_path = normalize_path(root_path)

   if is_s3_path(normalized_root_path):
      s3_root_path = re.sub('^s3(a|n)://', 's3://', normalized_root_path)
      script = "aws s3 ls --recursive {}/ | sed -E 's/[^\/]+$//' | awk '{{ print $4 }}' | sort | uniq | tail -2 | head -1".format(s3_root_path)
      latest_day_path = exec_script(script)

      s3_path = re.sub('^s3://[^/]+/', '', s3_root_path)
      latest_date = re.sub(s3_path, '', latest_day_path)
      latest_date = re.sub('^/', '', latest_date)
      latest_date = re.sub('/$', '', latest_date)
      return latest_date
   else:
      script = "find {} -type d | sort | tail -2 | head -1".format(normalized_root_path)
      latest_path = exec_script(script)
      latest_date = re.sub(root_path, '', latest_path)
      return latest_date

class AvroToParquetCompactor(object):

   def __init__(self, input_root_path, output_root_path):
      self.input_root_path = input_root_path
      self.output_root_path = output_root_path

   def execute(self):
      from pyspark.sql import SparkSession

      spark = SparkSession.builder.appName("AvroToParquetCompactor").getOrCreate()

      input_root_path = normalize_path(self.input_root_path)
      print("input_root_path = {}".format(input_root_path))

      output_root_path = normalize_path(self.output_root_path)
      print("input_root_path = {}".format(output_root_path))

      target_date = last_completed_date(input_root_path)
      print("target_date = {}".format(target_date))

      input_path = "{}/{}/*.avro".format(input_root_path, target_date)
      input_data_frame = spark.read.format("com.databricks.spark.avro").load(input_path)

      output_path = "{}/{}/".format(output_root_path, target_date)

      input_data_frame.write.mode("overwrite").parquet(output_path)

      spark.stop()

if __name__ == '__main__':
   arg_parser = configargparse.ArgParser()
   arg_parser.add('--input-root-path', required=True, help='path to input files grouped in YYYY-MM-DD/HH24 format')
   arg_parser.add('--output-root-path', required=True, help='path to output files grouped in YYYY-MM-DD/HH24 format')

   options = arg_parser.parse_args()
   compactor = AvroToParquetCompactor(**vars(options))
   compactor.execute()


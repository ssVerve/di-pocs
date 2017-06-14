import luigi
import string
import random

# def rand(size=6, chars=string.ascii_uppercase + string.digits):
#   return ''.join(random.choice(chars) for _ in range(size))

class DatedExternalTask(luigi.ExternalTask):
  date = luigi.DateParameter()
  def output(self): return luigi.LocalTarget("pocOutput/%s/%s.tsv" % (self.__class__.__name__, self.date))

class DWDimsStageToS3(DatedExternalTask): pass
class RTBLog(DatedExternalTask): pass
class AdcelLog(DatedExternalTask): pass

class DatedDummyTask(luigi.Task):
  date = luigi.DateParameter()
  def output(self): return luigi.LocalTarget("pocOutput/%s/%s.tsv" % (self.__class__.__name__, self.date))
  def run(self):
    with self.output().open('w') as outFile:
      for target in self.input():
        with target.open('r') as inFile:
          for line in inFile:
            outFile.write('%s-%s' % (self.__class__.__name__, line))

class DWDimsLoadToRedshift(DatedDummyTask):
  def requires(self): return [DWDimsStageToS3(self.date)]
class UpdateTargetTactic(DatedDummyTask):
  def requires(self): return [DWDimsLoadToRedshift(self.date)]
class FlightConfigsPois(DatedDummyTask):
  def requires(self): return [UpdateTargetTactic(self.date), DWDimsLoadToRedshift(self.date)]

class RTBIngest(DatedDummyTask):
  def requires(self): return [RTBLog(self.date)]
class AdcelRaw2Post(DatedDummyTask):
  def requires(self): return [AdcelLog(self.date)]
class RTBCleanAndTrim(DatedDummyTask):
  def requires(self): return [RTBIngest(self.date)]
class AdcelCleanAndTrim(DatedDummyTask):
  def requires(self): return [AdcelRaw2Post(self.date)]
class Sessions(DatedDummyTask):
  def requires(self): return [RTBCleanAndTrim(self.date), AdcelCleanAndTrim(self.date)]
class SummarizeGHs(DatedDummyTask):
  def requires(self): return [Sessions(self.date)]
class Visits(DatedDummyTask):
  def requires(self): return [SummarizeGHs(self.date), FlightConfigsPois(self.date)]


# class DateEmitter(luigi.Task):
#   date = luigi.DateParameter()
# 
#   def output(self):
#     return luigi.LocalTarget("pocOutput/DateEmitter_%s.tsv" % self.date)
#   def run(self):
#     with self.output().open('w') as outFile:
#       for _ in range(random.gauss(100, 20)):
#         outFile.write(id_generator(16))
# 
# class LocalDatedTask(luigi.Task):
#   date = luigi.DateParameter()
# 
#   def output(self):
#     return luigi.LocalTarget("pocOutput/LocalDatedTask_%s.tsv" % self.date)
# 
# 
#   def run(self):
#     with self.output().open('w') as outFile:
#       for target in self.input():
#         with target.open('r') as inFile:
#           for line in inFile:
#             outFile.write('%s:%s' % (str(self.date), line))

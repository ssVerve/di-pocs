import string
import random
from luigi import Task,ExternalTask,LocalTarget,DateParameter 
from luigi.contrib.s3 import S3FlagTarget
from vrvm.myboto.s3 import S3
from vrvm import ctrl_tbl_api


s3 = S3()

def rand(size=32, chars=string.ascii_uppercase + string.digits):
  return ''.join(random.choice(chars) for _ in range(size))

class DatedExternalTask(ExternalTask):
  date = DateParameter()
  def output(self): return LocalTarget("pocOutput/%s/%s.tsv" % (self.__class__.__name__, self.date))

class DWDimsStageToS3(DatedExternalTask): pass
class RTBLog(DatedExternalTask): pass
# class AdcelLog(DatedExternalTask): pass

class DatedDummyTask(Task):
  date = DateParameter()
  def output(self): return LocalTarget("pocOutput/%s/%s.tsv" % (self.__class__.__name__, self.date))
  def run(self):
    with self.output().open('w') as outFile:
      for target in self.input():
        with target.open('r') as inFile:
          for line in inFile:
            outFile.write('%s-%s' % (self.__class__.__name__, line))

class S3FlagDatedDummyTask(Task):
  date = DateParameter()
  def output(self): return S3FlagTarget('s3://verve-home/scottstewart/luigi/%s/%s/' % (self.__class__.__name__,self.date))
  def run(self):
    outPath = self.output().path
    for i in range(2): 
      s3.put(outPath + ('part-0000%s' % i), rand()) 
    s3.put(outPath + self.output().flag, '')
    
class S3FlagPrefixTarget(S3FlagTarget):
  def exists(self): return any(True for _ in s3._ls(self.path + self.flag))
    
class AdcelRaw2PostTarget(S3FlagTarget): # only checks for prefix of flag instead that key exists for flight-configs
  def __init__(self, path, date, ctrl_table_dsn='clientrds-dw-prod-control'):
    super(S3FlagTarget, self).__init__(path, flag=None)
    self.date = date
    self.ctrl_table_dsn = ctrl_table_dsn
  def exists(self): 
    return ctrl_tbl_api.CtrlTblApi(self.ctrl_table_dsn).count_unprocessed_logs('reporting', self.date) < 1

class DWDimsLoadToRedshift(DatedDummyTask):
  def requires(self): return [DWDimsStageToS3(self.date)]
class UpdateTargetTactic(DatedDummyTask):
  def requires(self): return [DWDimsLoadToRedshift(self.date)]
class FlightConfigsPois(S3FlagDatedDummyTask):
  def output(self): return S3FlagPrefixTarget('s3://verve-home/scottstewart/luigi/%s/' % (self.__class__.__name__), flag='updated_%s' % self.date)
  def requires(self): return [UpdateTargetTactic(self.date), DWDimsLoadToRedshift(self.date)]

class RTBIngest(S3FlagDatedDummyTask):
  def requires(self): return [RTBLog(self.date)]
class AdcelRaw2Post(S3FlagDatedDummyTask): # not really a flag task, doesn't currently write _SUCCESS, database check
  def output(self): return AdcelRaw2PostTarget('s3://verve-home/scottstewart/luigi/%s/%s/' % (self.__class__.__name__,self.date), self.date)
#   def requires(self): return [AdcelLog(self.date)]
class RTBCleanAndTrim(S3FlagDatedDummyTask):
  def requires(self): return [RTBIngest(self.date)]
class AdcelCleanAndTrim(S3FlagDatedDummyTask):
  def requires(self): return [AdcelRaw2Post(self.date)]
class Sessions(S3FlagDatedDummyTask):
  def requires(self): return [RTBCleanAndTrim(self.date), AdcelCleanAndTrim(self.date)]
class SummarizeGHs(S3FlagDatedDummyTask):
  def requires(self): return [Sessions(self.date)]
class Visits(S3FlagDatedDummyTask):
  def requires(self): return [SummarizeGHs(self.date), FlightConfigsPois(self.date)]


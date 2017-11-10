while read line
do
        hadoop fs -text $line | xargs -I msg curl -d 'msg' 'http://ip-10-2-99-249.diprod.us-east-1.aws.vrv:4151/pub?topic=rtbnobid'
done

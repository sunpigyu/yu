import os
import sys

write = open("/var/spool/cron/hadoop_new",'a')
lines = open("/var/spool/cron/hadoop",'r')
for line in lines:
	if (line.startswith('#')):
		continue
	else :
		write.write(line+"\n")
lines.close()
write.close()

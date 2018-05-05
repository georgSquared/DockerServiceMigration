import os, time
import subprocess

print 'Monitoring Load'
while True:
	load_avg = os.getloadavg()

	if load_avg[0] > 0.9:
		print "Load average of last minute too high"
		print "Leaving Swarm"
		
		subprocess.call(['docker', 'swarm', 'leave'])

		break

	time.sleep(1)
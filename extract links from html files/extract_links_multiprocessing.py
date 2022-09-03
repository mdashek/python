# import libraries
import multiprocessing
import re
import csv
import glob
import numpy as np
import pandas as pd
import datetime

parallel_process = 5

def links_extraction(files, divider, rem):

	file_list = []
	for seq in range(len(files)):
		if seq % divider == rem:
				file_list.append(files[seq])
	
	# open output csv file in the write mode
	f = open('outputresults_' + str(rem) + '.csv', 'w', newline='', encoding="utf-8")
	# create the csv writer
	writer = csv.writer(f)
	i = 0
	# read each file and extract links
	for file in file_list:		
		# Opening the html file
		HTMLFile = np.genfromtxt(file, encoding="utf8", dtype='str', delimiter="\t")
	
		for line in HTMLFile:
			links = re.findall('"((http)s?://.*?)"', line)
			
			# write links in output file
			for link in links:
				if link[0] != 'http://' and link[0] != 'https://' and link[0] != '':
					# write a row to the csv file
					writer.writerow([link[0]])
	
	# close the file
	f.close()

if __name__=="__main__":
	start = datetime.datetime.now()
	print("Starting task at {}".format(start.strftime("%H:%M:%S %d-%m-%Y")))
	files = glob.glob("Inputfolder/**/*.htm", recursive = True)
	print('File count: ' + str(len(files)))

	subprocess_list = []
	for rem in range(parallel_process):
		proc = multiprocessing.Process(target=links_extraction, args=(files, parallel_process, rem,))
		proc.start()
		subprocess_list.append(proc)

	for prc in subprocess_list:
			prc.join()

	end = datetime.datetime.now()
	diff = end - start
	c = diff
	print("Task finished at {}\nExecution time {}".format(end.strftime("%H:%M:%S %d-%m-%Y"), str(datetime.timedelta(seconds=c.seconds))))
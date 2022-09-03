# import libraries
import re
import csv
import glob

# identify sub folders
subfolders = glob.glob("Inputfolder/*/", recursive = True)
print('List of subfolders:')
for subfolder in subfolders:
	print(subfolder[:-1].replace('Inputfolder\\', ''))

# extract links for each subfolder iteratively
for subfolder in subfolders:
	# list all files in a subfolder
	files = glob.glob(subfolder + '*.htm')
	print('Subfolder name: ' + subfolder[:-1].replace('Inputfolder\\', ''))
	print('File count: ' + str(len(files)))
	
	# open output csv file in the write mode
	f = open('outputresults_' + subfolder[:-1].replace('Inputfolder\\', '') + '.csv', 'w', newline='', encoding="utf-8")
	# create the csv writer
	writer = csv.writer(f)
	i = 0
	# read each file and extract links
	for file in files:		
		# Opening the html file
		HTMLFile = open(file, "r", encoding="utf8")
		  
		# Reading the file
		html = HTMLFile.read()

		# use re.findall to get all the links
		links = re.findall('"((http)s?://.*?)"', html)
		
		# write links in output file
		for link in links:
			if link[0] != 'http://' and link[0] != 'https://':
				# write a row to the csv file
				writer.writerow([link[0].strip()])

		# close html file
		HTMLFile.close()
	
	# close the file
	f.close()


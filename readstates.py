#!/usr/bin/python

################################################
#                                              #
#              qtop v.0.1.1                    #
#                                              #
#     Licensed under MIT-GPL licenses          #
#                                              #
#                     Fotis Georgatos, ????    #
#                     Sotiris Fragkiskos, CERN #
################################################

"""

changelog:
=========

0.1.2: script reads sleep0.out files from each job and displays status for each job
0.1.1: changed implementation in get_state()

0.1.0: just read a pbsnodes-a output file and gather the results in a single line


"""


import sys,os,glob



"""

def write_state(fout):
    with open('fout', mode='a') as file:
        string=status
        file.write(string)
"""



def get_state(fin):
    """
    this gets the state of the nodes for each given file-job.
    """
    status=''
    for line in fin:
        line.strip()
        if line.find('state = ')!=-1:
            nextchar=line.split()[3][0]
            if nextchar=='f': status+='-'
            else:
                status+=nextchar
    return status


if __name__ == "__main__":
	
	DIR='~/sleep-oldVersion/outputs/' #where the output for each job is stored
	mypath=os.path.expanduser(DIR)
	os.chdir(mypath)
	outputDirs=glob.glob('sfragk*')	
	outputFiles=[]
	for dir in outputDirs:
		os.chdir(dir)
		if glob.glob('*.out'): #is there actually an output from the job?
			outputFile=glob.glob('*.out')[0]
			outputFiles.append(os.path.join(DIR,dir,outputFile))
		os.chdir('..')

	for File in outputFiles:
		File=os.path.expanduser(File)
		fin=open(File,"r")	
		print get_state(fin)
        print
    	fin.close()



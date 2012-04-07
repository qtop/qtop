#!/usr/bin/env python

################################################
#                                              #
#              qtop v.0.1.3                    #
#                                              #
#     Licensed under MIT-GPL licenses          #
#                                              #
#                     Fotis Georgatos          #
#                     Sotiris Fragkiskos       #
################################################

"""

changelog:
=========

0.1.3: fixed tabs-to-spaces. Formatting should be correct now.
       Now each state is saved in a separate file in a results folder
0.1.2: script reads sleep0.out files from each job and displays status for each job
0.1.1: changed implementation in get_state()

0.1.0: just read a pbsnodes-a output file and gather the results in a single line


"""


import sys,os,glob

if not os.path.exists(os.path.expanduser('~/qstat-job/results')):
    cmd='mkdir ~/qstat-job/results'
    fp = os.popen(cmd)

savedir='~/qstat-job/results'
savedir=os.path.expanduser(savedir)

    





def write_state(filename,state):
    fout=open(filename,'w')
    #try:
    fout.write(state)
    #finally:
    fout.close()
    




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
    fin.close() 
    return status


if __name__ == "__main__":
    
    outputDirs, outputFiles=[],[]

    outputpath='~/qstat-job/outputs/' #where the output for each job is stored
    outputpath=os.path.expanduser(outputpath)
    os.chdir(outputpath)

    outputDirs+=glob.glob('sfragk*') 

    for dir in outputDirs:
        '''
        create full path to each sleep.out file and put in outputFiles list
        '''
        os.chdir(dir)
        if glob.glob('*.out'): #is there an actual output from the job?
            outputFile=glob.glob('*.out')[0]
            outputFiles.append(os.path.join(outputpath,dir,outputFile))
        os.chdir('..')

    for fullname in outputFiles:
        '''
        get state for each job and write to separate file in results directory
        '''
        fullname=os.path.expanduser(fullname)
        (dirname, filename) = os.path.split(fullname)
        fin=open(fullname,"r")  
        getst = get_state(fin)
        #print getst  #--> jjjjj-----d-d----- etc
        save=dirname
        (outdir,statefile)=os.path.split(save)
        os.chdir(savedir)
        #print os.getcwd() # --> results dir
        write_state(statefile,getst)

#!/usr/bin/env python

################################################
#                                              #
#              qtop v.0.1.5                    #
#                                              #
#     Licensed under MIT-GPL licenses          #
#                                              #
#                     Fotis Georgatos          #
#                     Sotiris Fragkiskos       #
################################################

"""

changelog:
=========
0.1.5: implemented saving to 3 separate files, qstat.out, qstat-q.out, pbsnodes.out
0.1.4: some "wiremelting" concerning the save directory
0.1.3: fixed tabs-to-spaces. Formatting should be correct now.
       Now each state is saved in a separate file in a results folder
0.1.2: script reads qtop-input.out files from each job and displays status for each job
0.1.1: changed implementation in get_state()

0.1.0: just read a pbsnodes-a output file and gather the results in a single line


"""


import sys,os,glob

savedir='~/qtop-input/results'
savedir=os.path.expanduser(savedir)

outputpath='~/qtop-input/outputs/' #where the output for each job is stored
outputpath=os.path.expanduser(outputpath)
    

if not os.path.exists(savedir):
    cmd='mkdir '+savedir
    fp = os.popen(cmd)   #execute cmd 'mkdir /home/sfragk/qtop-input/results'

    
def get_state(fin):
    """
    this gets the state of each of the nodes for each given file-job (*.out), appends it to variable status and
    returns the status, which is of the form e.g. ----do---dddd
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

def write_string(filename,something):
    '''
    appends the contents of a variable to a file 
    '''
    fout=open(filename,'w') #itan 'w', to allaja gia na ti xrisimopoiisw kai stin writefromtoseparate
    fout.write(something)
    fout.close()

def writeToSeparate(filename1,filename2):
     '''
     writes the data from qstat,qstat-q,pbsnodes, which all reside in
     qtop-input.out, to a file with the corresponding name, first taking out the prepended command
     '''
     fin=open(filename1,'r')
     fout=open(filename2,'w')
     for line in fin:
        if line.startswith(filename2.split('.')[0]+':'):
            fout.write(line.split(':',1)[1])
     fin.close() 

if __name__ == "__main__":
    
    outputDirs, outputFiles=[],[]

    os.chdir(outputpath)

    outputDirs+=glob.glob('sfragk*') 

    for dir in outputDirs:
        '''
        create full path to each qtop-input.out file and put it in outputFiles list
        '''
        os.chdir(dir)
        if glob.glob('*.out'): #is there an actual output from the job?
            outputFile=glob.glob('*.out')[0]
            outputFiles.append(os.path.join(outputpath,dir,outputFile))
            '''
            thelw na epipleon na parw ka8e outputFile kai na to spasw se 3.
            '''
            sepFiles=['pbsnodes.out','qstat.out','qstat-q.out']
            for sepFile in sepFiles:
                writeToSeparate(outputFile,sepFile)
                
        os.chdir('..')

'''
    for fullname in outputFiles:
        #get state for each job and write it to a separate file in results directory
        fullname=os.path.expanduser(fullname)
        (dirname, filename) = os.path.split(fullname)
        fin=open(fullname,"r")  
        getst = get_state(fin)
        #print getst  #--> jjjjj-----d-d----- etc
        save=dirname
        (outdir,statefile)=os.path.split(save)
        os.chdir(savedir)
        #print os.getcwd() # --> results dir
        #write_string(statefile,getst)
'''

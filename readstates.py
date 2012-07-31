#!/usr/bin/env python

################################################
#                                              #
#              qtop v.0.2.3                    #
#                                              #
#     Licensed under MIT-GPL licenses          #
#                                              #
#                     Fotis Georgatos          #
#                     Sotiris Fragkiskos       #
################################################

"""

changelog:
=========
0.2.3: corrected regex search pattern in MakeQstat to recognize usernames like spec101u1 (number followed by number followed by letter)
       now handles non-uniform setups
       R+Q / all: all did not display everything (E status)
0.2.2: clipping functionality
0.2.1: Hashes displaying when the node has less cores than the max declared by a WN (its np variable)
0.2.0: unix accounts are now correctly ordered
0.1.9: All CPU lines displaying correctly 
0.1.8: unix account id assignment to CPU0,1 implemented
0.1.7: ReadQstatQ function (write in yaml format using Pyyaml)
       output up to Node state !
0.1.6: ReadPbsNodes function (write in yaml format using Pyyaml)
0.1.5: implemented saving to 3 separate files, qstat.out, qstat-q.out, pbsnodes.out
0.1.4: some "wiremelting" concerning the save directory
0.1.3: fixed tabs-to-spaces. Formatting should be correct now.
       Now each state is saved in a separate file in a results folder
0.1.2: script reads qtop-input.out files from each job and displays status for each job
0.1.1: changed implementation in get_state()

0.1.0: just read a pbsnodes-a output file and gather the results in a single line


"""


import sys,os,glob,re,yaml,datetime,itertools
from operator import itemgetter
#from readpbsyaml import *

#savedir='~/qtop-input/results'
#savedir=os.path.expanduser(savedir)

outputpath='~/qtop-input/outputs/' #where the output for each job is stored
homepath=os.path.expanduser('~/')
outputpath=os.path.expanduser(outputpath)
    

#if not os.path.exists(savedir):
#    cmd='mkdir '+savedir
#    fp = os.popen(cmd)   #execute cmd 'mkdir /home/sfragk/qtop-input/results'

CLIPPING = True
RMWARNING = '=== WARNING: --- Remapping WN names and retrying heuristics... good luck with this... ---'
remapnr=0
nodeinitsDic=dict()
outputDirs=[]
statelst=[]
maxcores=0
#qstatqdic={} # fainetai na xrisimopoieitai mono mia fora, xrisimopoiw to qstatqLst instead
wndic, wndicrm={}, {}
dname=''
BiggestWrittenNode=0
wnlist, wnlistrm=[], []
nodenr=''
lastnode=0
ExistingNodes, NonExistingNodes, OfflineDownNodes=0, [], 0
TotalCores=0
QueueName,Mem,CPUtime,Walltime,Node,Run,Queued,Lm,State,TotalRuns,TotalQueues=0,0,0,0,0,0,0,0,0,0,0 #for readQstatQ
Jobid,Jobnr,CEname,Name,User,TimeUse,S,Queue=0,0,'','','','','','' #for readQstat
qstatqLst,qstatLst=[],[]
bigjoblist=[]
UserOfJobId={}
CoreOfJob={}
IdOfUnixAccount={}

def writeToSeparate(filename1,filename2):
    '''
    writes the data from qstat,qstat-q,pbsnodes, which all reside in
    qtop-input.out, to a file with the corresponding name, first taking out the prefix in each line.
    '''
    fin=open(filename1,'r')
    fout=open(filename2,'w')
    for line in fin:
        if line.startswith(filename2.split('.')[0]+':'):
            fout.write(line.split(':',1)[1])
    fin.close() 


'''
def get_state(fin):     #yamlstream
    """
    gets the state of each of the nodes for each given file-job (pbsnodes.yaml), appends it to variable 'status' and
    returns the status, which is of the form e.g. ----do---dddd
    """
    state=''
    for line in fin:
        line.strip()
        # if line.find('state: ')!=-1:
        if 'state: ' in line: 
            nextchar=line.split()[1].strip("'")
            if nextchar=='f': state+='-'
            else:
                state+=nextchar
    fin.close() 
    statelst=list(state)
    return statelst
    #or
    #return state
'''
#yamlstream=open('/home/sfranky/qt/pbsnodes.yaml', 'r')
#statebeforeUnsorted=get_state(yamlstream)    

"""
def get_corejobs(fin):   #yamlstream
    core0state,core1state='',''
    for line in fin:
        line.strip()
        if line.find("core: '0'")!=-1:
            jobcpu1=fin.readline().split()[1]
        elif line.find("core: '1'")!=-1:
            jobcpu2=fin.readline().split()[1]
        elif line.find("core: '2'")!=-1:
            jobcpu3=fin.readline().split()[1]
        elif line.find("core: '3'")!=-1:
            jobcpu4=fin.readline().split()[1]
"""

def MakePbsNodesyaml(fin,fout):
    """
    read pbsnodes.out sequentially and put in respective yaml file
    """
    global OfflineDownNodes # NonExistingNodes, bigjoblist
    
    # nodenr=0
    for line in fin:
        line.strip()
        searchdname='^\w+(\.\w+)*'
        if re.search(searchdname, line)!=None:   # line containing domain name
            m=re.search(searchdname, line) #i was missing a "^" here in the beginning of the searchdname actual string
            dname=m.group(0)
            # print 'dname in makepbs is ', dname
            fout.write('domainname: ' + dname + '\n')

        elif 'state = ' in line:  #line.find('state = ')!=-1:
            nextchar=line.split()[2][0]
            if nextchar=='f': 
                state='-'
            elif (nextchar=='d')|(nextchar=='o'):
                state=nextchar
                OfflineDownNodes+=1
            else:
                state=nextchar
            fout.write('state: ' + state + '\n')

        elif 'np = ' in line:   #line.find('np = ')!=-1:
            np=line.split()[2][0:]
            #TotalCores=int(np)
            fout.write('np: ' + np + '\n')
            

        elif 'jobs = ' in line:    #line.find('jobs = ')!=-1:
            ljobs=line.split('=')[1].split(',')
            ####joblist=[]
            prev_job=0
            for job in ljobs:
                core=job.strip().split('/')[0]
                job=job.strip().split('/')[1:][0].split('.')[0]
                fout.write('- core: ' + core +'\n')
                fout.write('  job: ' + job +'\n')
                #CoreOfJob[job]=core #remember to transfer to ReadPbsNodesyaml

        elif 'gpus = ' in line:     #line.find('gpus = ')!=-1:
            gpus=line.split(' = ')[1]
            fout.write('gpus: ' + gpus + '\n')

        elif line.startswith('\n'):
            fout.write('\n')

    fin.close()
    fout.close()

def ReadPbsNodesyaml(fin):
    '''
    extracts highest node number, online nodes
    '''
    global ExistingNodes, NonExistingNodes, OfflineDownNodes, lastnode, bigjoblist, jobseries,BiggestWrittenNode,wnlist, wnlistrm,nodenr, TotalCores, CoreOfJob, wndic, wndicrm, maxcores,maxnp,statelst,nodeinitsDic, remapnr

    maxcores = 0
    maxnp = 0
    state=''
    county=0
    for line in fin:
        line.strip()
        county+=1
        searchdname='domainname: '+'(\w+(\.\w+)*)'
        searchnodenr='([A-Za-z]+)(\d+)'
        # print 'take #', county
        if re.search(searchdname, line)!=None:   # line containing domain name
            # case=0
            m=re.search(searchdname, line) #i was missing a "^" here in the beginning of the searchdname actual string
            dname=m.group(1)
            remapnr+=1
            # print 'dname in readpbs is ', dname
            # print 'm.group0 is  ', m.group(0)
            # print 'dname is ', dname
            '''
            extract highest node number,online nodes
            '''
            ExistingNodes+=1    #nodes as recorded on pbsnodes.out
            # print 'line is ', line
            if re.search(searchnodenr, dname)!=None:
                n=re.search(searchnodenr, dname)
                nodenr=int(n.group(2))
                nodeinits=n.group(1)
                nodeinitsDic.setdefault(nodeinits,0)    # for non-uniform setups of WNs, eg g01... and n01...
                # print 'dname is ', dname 
                # print 'nodenr is ', nodenr
                # print 'nodeinits are ', nodeinits
                # print 'node nr is ' + nodenr
                wndic[nodenr]=[]
                wndicrm[remapnr]=[]
                if nodenr > BiggestWrittenNode:
                    BiggestWrittenNode=nodenr
                wnlist.append(nodenr)
                wnlistrm.append(remapnr)
        elif 'state: ' in line: 
            # case=2
            nextchar=line.split()[1].strip("'")
            if nextchar=='f': 
                state+='-'
                wndic[nodenr].append('-')
                wndicrm[remapnr].append('-')
            else:
                state+=nextchar
                wndic[nodenr].append(nextchar)
                wndicrm[remapnr].append(nextchar)
            
        elif 'np:' in line:
            # case=3            
            np=line.split(': ')[1].strip()
            wndic[nodenr].append(np)
            wndicrm[remapnr].append(np)
            if int(np)>int(maxnp):
                maxnp=int(np)
            TotalCores+=int(np)
        elif 'core: ' in line:
            # case=4            
            core=line.split(': ')[1].strip()
            if int(core)>int(maxcores):
                maxcores=int(core)
        elif 'job: ' in line:
            # case=5            
            job=str(line.split(': ')[1]).strip()
            wndic[nodenr].append((core,job))
            wndicrm[remapnr].append((core,job))
            #prev_job=0
        # print 'successful case was ', case

    statelst=list(state)
    lastnode = BiggestWrittenNode
    maxcores+=1
    #if maxnp > maxcores:      # 
    #    maxcores=maxnp        # auto to krataw?         

    '''
    fill in invisible WN nodes with '?'   14/5
    and count them
    '''
    if len(nodeinitsDic) >1:
        for i in range(1,remapnr):
            if i not in wndicrm:
                wndicrm[i]='?'
                NonExistingNodes.append(i)
    else:
        for i in range(1,BiggestWrittenNode):
            if i not in wndic:
                wndic[i]='?'
                NonExistingNodes.append(i)

    wnlist.sort()
    wnlistrm.sort()
    diff=0
    # biggestlst=[]
    # for i in range(1,BiggestWrittenNode+1): biggestlst.append(i)
    # #print len(nodelist), len(biggestlst)
    # if len(wnlist)!=len(biggestlst):
    #     wnlist.extend(['?']*(len(biggestlst)-len(wnlist)))
    # if sorted(wnlist)!=sorted(biggestlst):
    #     for node,bignode in zip(wnlist,biggestlst):
    #         pass   #print node,bignode

    
def MakeQstatQ(fin,fout):
    global QueueName,Mem,CPUtime,Walltime,Node,Run,Queued,Lm,State,TotalRuns,TotalQueues,qstatqLst
    """
    read qstat-q.out sequentially and put useful data in respective yaml file
    """
    Queuesearch='^([a-z]+)\s+(--)\s+(--|\d+:\d+:\d+)\s+(--|\d+:\d+:\d+)\s+(--)\s+(\d+)\s+(\d+)\s+(--|\d+)\s+([DE] R)'
    RunQdSearch='^\s*(\d+)\s+(\d+)'
    for line in fin:
        line.strip()
        # searches for something like: biomed             --      --    72:00:00   --   31   0 --   E R
        if re.search(Queuesearch, line)!=None:
            m=re.search(Queuesearch, line)
            _,QueueName,Mem,CPUtime,Walltime,Node,Run,Queued,Lm,State=m.group(0),m.group(1),m.group(2),m.group(3),m.group(4),m.group(5),m.group(6),m.group(7),m.group(8),m.group(9)
            qstatqLst.append((QueueName,Run,Queued,Lm,State))   # which one to keep?  #remember to move to ReadQstatQ
            #qstatqdic[QueueName]=[(Run,Queued,Lm,State)]    # which one to keep?  #remember to move to ReadQstatQ
            fout.write('- QueueName: ' + QueueName +'\n')
            fout.write('  Running: ' + Run +'\n')
            fout.write('  Queued: ' + Queued +'\n')
            fout.write('  Lm: ' + Lm +'\n')
            fout.write('  State: ' + State +'\n')
            fout.write('\n')
        elif re.search(RunQdSearch, line)!=None:
            n=re.search(RunQdSearch, line)
            TotalRuns, TotalQueues=n.group(1), n.group(2)
    fout.write('---\n')
    fout.write('Total Running: ' + TotalRuns + '\n')
    fout.write('Total Queued: ' + TotalQueues + '\n')

def MakeQstat(fin,fout):
    global Jobid,Jobnr,CEname,Name,User,TimeUse,S,Queue,Id2Unix
    """
    read qstat.out sequentially and put useful data in respective yaml file
    """
    UserQueueSearch='^((\d+)\.([A-Za-z-]+[0-9]*))\s+([A-Za-z0-9_.-]+)\s+([A-Za-z0-9]+)\s+(\d+:\d+:\d*|0)\s+([CWRQE])\s+(\w+)'
    RunQdSearch='^\s*(\d+)\s+(\d+)'
    for line in fin:
        line.strip()
        # searches for something like: 422561.cream01             STDIN            see062          48:50:12 R see       
        if re.search(UserQueueSearch, line)!=None:
            m=re.search(UserQueueSearch, line)
            Jobid,Jobnr,CEname,Name,User,TimeUse,S,Queue=m.group(1),m.group(2),m.group(3),m.group(4),m.group(5),m.group(6),m.group(7),m.group(8)
            qstatLst.append([[Jobnr],User,S,Queue])
            Jobid=Jobid.split('.')[0]
            fout.write('---\n')
            fout.write('JobId: ' + Jobid +'\n')    
            fout.write('UnixAccount: ' + User +'\n')
            fout.write('S: ' + S +'\n')
            fout.write('Queue: ' + Queue +'\n')

            # UnixOfJobId[Jobid.split('.')[0]]=User
            UserOfJobId[Jobid]=User
            fout.write('...\n')

############################################################################################

#empties the files with every run of the python script
fin1temp=open(homepath+'qt/pbsnodes.yaml','w')
fin1temp.close()

fin2temp=open(homepath+'qt/qstat-q.yaml','w')
fin2temp.close()

fin3temp=open(homepath+'qt/qstat.yaml','w')
fin3temp.close()



os.chdir(outputpath)
outputDirs+=glob.glob('sfragk*') 
outputDirs+=glob.glob('fotis*') 


for dir in outputDirs:
    # if dir=='fotistestfiles': #OK
    # if dir=='sfragk_tEbjFj59gTww0f46jTzyQA':  # implement clip/masking functionality !! OK
    # if dir=='sfragk_sDNCrWLMn22KMDBH_jboLQ':  #OK
    #if dir=='sfragk_aRk11NE12OEDGvDiX9ExUg':   # OK
    # if dir=='sfragk_gHYT96ReT3-QxTcvjcKzrQ':  # OK
    # if dir=='sfragk_zBwyi8fu8In5rLu7RBtLJw':  # OK
    # if dir=='sfragk_xq9Z9Dw1YU8KQiBu-A5sQg':  #OK
    # if dir=='sfragk_sE5OozGPbCemJxLJyoS89w':  # seems ok !
    # if dir=='sfragk_vshrdVf9pfFBvWQ5YfrnYg':  # ##s ?
    if dir=='sfragk_R__ngzvVl5L22epgFVZOkA':  # ##s instead of __s, wrong node state (no ??)
    # if dir=='sfragk_iLu0q1CbVgoDFLVhh5NGNw': # diaforetiko me tou foti

        os.chdir(dir)
        yamlstream1=open(homepath+'qt/pbsnodes.yaml', 'a')
        yamlstream2=open(homepath+'qt/qstat-q.yaml', 'a')
        yamlstream3=open(homepath+'qt/qstat.yaml', 'a')

        fin1=open('pbsnodes.out','r')
        MakePbsNodesyaml(fin1,yamlstream1)
        yamlstream1=open(homepath+'qt/pbsnodes.yaml','r')
        ReadPbsNodesyaml(yamlstream1)
        yamlstream1.close()

        fin2=open('qstat-q.out','r')
        MakeQstatQ(fin2,yamlstream2)
        fin2.close()
        yamlstream2.close()

        fin3=open('qstat.out','r')
        MakeQstat(fin3,yamlstream3)
        fin3.close()
        yamlstream3.close()

        os.chdir('..')

#os.chdir(homepath+'inp/outputs/sfragk_aRk11NE12OEDGvDiX9ExUg/')            
#fin=open('pbsnodes.out','r')            
#ReadPbsNodes(fin,yamlstream)


'''
if __name__ == "__main__":
    
    outputDirs, outputFiles=[],[]

    os.chdir(outputpath)
    outputDirs+=glob.glob('sfragk*') 

    for dir in outputDirs:
        #create full path to each sfragk_31sdf.../qtop-input.out file and put it in list outputFiles 
        os.chdir(dir)
        if glob.glob('*.out'): #is there an actual output from the job?
            outputFile=glob.glob('*.out')[0]
            outputFiles.append(os.path.join(outputpath,dir,outputFile))
            #here is where each .out file is broken into 3 files
            sepFiles=['pbsnodes.out','qstat.out','qstat-q.out']
            for sepFile in sepFiles:
                writeToSeparate(outputFile,sepFile)
        os.chdir('..')

    yfile=('pbsnodes.out', 'r')
    ReadPbsNodes(yfile)
'''

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
        #write_string(statefile,getst)  # need to change this as I deleted write_string. make a fin=open(statefile) etc
'''

#### QTOP  DISPLAY #######################

if len(nodeinitsDic) >1:
    print RMWARNING
print 'PBS report tool. Please try: watch -d /home/sfragk/off/qtop . All bugs added by fotis@cern.ch. Cross fingers now...\n'
print '===> Job accounting summary <=== (Rev: 3000 $) %s WORKDIR=to be added\n' % (datetime.datetime.today())
print 'Usage Totals:\t%s/%s\t Nodes | x/%s\t Cores |\t %s+%s\t jobs (R+Q) reported by qstat -q' %(ExistingNodes-OfflineDownNodes, ExistingNodes, TotalCores, int(TotalRuns), int(TotalQueues) )
#print 'Queues: | '+elem[0]+': '+elem[1]+'+'+elem[2]+' \n' % [elem[0] for elem in qstatqLst], [elem[1] for elem in qstatqLst], [elem[2] for elem in qstatqLst]
print 'Queues: | ',
for i in qstatqLst:
    print i[0]+': '+i[1]+'+'+i[2]+' |',
print '* implies blocked'
print '\n'
print '===> Worker Nodes occupancy <=== (you can read vertically the node IDs; nodes in free state are noted with - )'

#prints the worker node ID number lines


if len(nodeinitsDic)>1:
    if remapnr<10:
        unit=str(remapnr)[0]
    elif remapnr<100:
        dec=str(remapnr)[0]
        unit=str(remapnr)[1]
    elif remapnr<1000:
        cent=int(str(remapnr)[0])
        dec=int(str(remapnr)[1])
        unit=int(str(remapnr)[2])
else:
    if lastnode<10:
        unit=str(lastnode)[0]
    elif lastnode<100:
        dec=str(lastnode)[0]
        unit=str(lastnode)[1]
    elif lastnode<1000:
        cent=int(str(lastnode)[0])
        dec=int(str(lastnode)[1])
        unit=int(str(lastnode)[2])    

c,d,d_,u='','','',''
beginprint=0

#if there are non-uniform WNs in pbsnodes.yaml, remapping is performed
if len(nodeinitsDic) == 1:

    if lastnode<10:
        for node in range(lastnode):
            u+= str(node+1)
        print u+'={__WNID__}'
    elif lastnode<100:
        d_='0'*9+'1'*10+'2'*10+'3'*10+'4'*10+'5'*10+'6'*10+'7'*10+'8'*10+'9'*10
        ud='1234567890'*10
        d=d_[:lastnode]
        print d+            '={_Worker_}'
        print ud[:lastnode]+'={__Node__}'
    elif lastnode<1000:
        c+=str(0)*99
        for i in range(1,cent):
            c+=str(i)*100
        c+=str(cent)*(int(dec))*10 + str(cent)*(int(unit)+1)
        
        d_='0'*9+'1'*10+'2'*10+'3'*10+'4'*10+'5'*10+'6'*10+'7'*10+'8'*10+'9'*10
        d=d_
        for i in range(1,cent):
            d+=str(0)+d_
        else:
            d+=str(0)
        d+=d_[:int(str(dec)+str(unit))]
        
        uc='1234567890'*100
        ua=uc[:lastnode]

        #clipping functionality:
        '''
        if the earliest node number is high (e.g. 80), the first 79 WNs need not show up.
        '''
        beginprint=0
        if (CLIPPING == True) and wnlist[0]>30:
            beginprint=wnlist[0]-1
        print c[beginprint:] + '={_Worker_}'
        print d[beginprint:] + '={__Node__}'
        print ua[beginprint:]+ '={___ID___}'
        #todo: remember to fix <100 cases (do i really need to, though?)
elif len(nodeinitsDic) >1:
    if remapnr<10:
        for node in range(remapnr):
            u+= str(node+1)
        print u+'={__WNID__}'
    elif remapnr<100:
        d_='0'*9+'1'*10+'2'*10+'3'*10+'4'*10+'5'*10+'6'*10+'7'*10+'8'*10+'9'*10
        ud='1234567890'*10
        d=d_[:remapnr]
        print d+            '={_Worker_}'
        print ud[:remapnr]+'={__Node__}'
    elif remapnr<1000:
        c+=str(0)*99
        for i in range(1,cent):
            c+=str(i)*100
        c+=str(cent)*dec*10 + str(cent)*(unit+1)
        
        d_='0'*9+'1'*10+'2'*10+'3'*10+'4'*10+'5'*10+'6'*10+'7'*10+'8'*10+'9'*10
        d=d_
        for i in range(1,cent):
            d+=str(0)+d_
        else:
            d+=str(0)
        d+=d_[:int(str(dec)+str(unit))]
        
        uc='1234567890'*100
        ua=uc[:remapnr]

        #clipping functionality:
        '''
        if the earliest node number is high (e.g. 80), the first 79 WNs need not show up.
        '''
        beginprint=0
        if (CLIPPING == True) and wnlistrm[0]>30:
            beginprint=wnlist[0]-1
        print c[beginprint:] + '={_Worker_}'
        print d[beginprint:] + '={__Node__}'
        print ua[beginprint:]+ '={___ID___}'
        #todo: remember to fix <100 cases (do i really need to, though?)
    


##end of code outputting workernode id number lines
###################################################


# 14/6  alternative solution for extra 'invisible' WNs
yamlstream=open(homepath+'qt/pbsnodes.yaml', 'r')
# statebeforeUnsorted=statelst               #get_state(yamlstream)
# if NonExistingNodes:
#     for nonex in NonExistingNodes:
#         statebeforeUnsorted.insert(nonex-1,'?')
#         # stateafter=statebeforeUnsorted[:NonExistingNodes[0]-1]+'?'+statebeforeUnsorted[NonExistingNodes[0]-1:]
#         stateafter=statebeforeUnsorted
# else:
#     stateafter=statebeforeUnsorted

# stateafterstr=''
# for i in stateafter:    #or stateafterstr=''.join(stateafter)
#     stateafterstr+=i


stateafterstr=''
if len(nodeinitsDic) ==1:
    for node in wndic:  #why are dictionaries ALWAYS ordered when keys are '1','5','3' etc ?!!?!?
        stateafterstr+=wndic[node][0]
elif len(nodeinitsDic)>1:
    for node in wndicrm:  #why are dictionaries ALWAYS ordered when keys are '1','5','3' etc ?!!?!?
        stateafterstr+=wndicrm[node][0]

print stateafterstr[beginprint:]+'=Node state'

yamlstream.close()

#############################################

#kati san def readqstat ?
JobIds=[]
UnixAccounts=[]
Ss=[]
Queues=[]
finr=open(homepath+'qt/qstat.yaml', 'r')
for line in finr:
    if line.startswith('JobId:'):
        JobIds.append(line.split()[1])
        # JobIds.append(line.split()[2].split()[0])
    elif line.startswith('UnixAccount:'):
        UnixAccounts.append(line.split()[1])
        # UnixAccounts.append(line.split()[2])
    elif line.startswith('S:'):           
        Ss.append(line.split()[1])
        # Ss.append(line.split()[2])
    elif line.startswith('Queue:'):           
        Queues.append(line.split()[1])
        # Queues.append(line.split()[2])
finr.close()

#antistoixisi unix account me to jobid tou
User2JobDic={}
for user,jobid in zip(UnixAccounts,JobIds):
    User2JobDic[jobid]=user

'''
yamlstream=open(homepath+'qt/pbsnodes.yaml', 'r')
statebeforeUnsorted=get_state(yamlstream)
stateafter=statebeforeUnsorted[:nonodes[0]-1]+'?'+statebeforeUnsorted[nonodes[0]-1:]
for i in range(1,len(nonodes)):
    stateafter=stateafter[:nonodes[i]-1]+'?'+stateafter[nonodes[i]-1:]
print stateafter+'=Node state'
'''


#solution for counting R,Q,C attached to each user
UserRunningDic, UserQueuedDic, UserCancelledDic, UserWaitingDic, UserEDic = {}, {}, {}, {}, {}

for user,status in zip(UnixAccounts,Ss):
    if status=='R':
        UserRunningDic[user] = UserRunningDic.get(user, 0) + 1
    elif status=='Q':
        UserQueuedDic[user] = UserQueuedDic.get(user, 0) + 1
    elif status=='C':
        UserCancelledDic[user] = UserCancelledDic.get(user, 0) + 1
    elif status=='W':
        UserWaitingDic[user] = UserWaitingDic.get(user, 0) + 1
    elif status=='E':
        UserWaitingDic[user] = UserEDic.get(user, 0) + 1

for account in UserRunningDic:
    UserQueuedDic.setdefault(account, 0)
    UserCancelledDic.setdefault(account, 0)
    UserWaitingDic.setdefault(account, 0)
    UserEDic.setdefault(account, 0)

occurencedic={}
for user in UnixAccounts:
    occurencedic[user]=UnixAccounts.count(user)

Usersortedlst=sorted(occurencedic.items(), key=itemgetter(1), reverse = True)


#IdOfUnixAccount = {}
j=0
possibleIDs='0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
for unixaccount in Usersortedlst:
    IdOfUnixAccount[unixaccount[0]]=possibleIDs[j]
    j+=1
########################## end of copied from below

flatjoblist=[]
flattened = itertools.chain.from_iterable(bigjoblist)
for i in flattened:
    flatjoblist.append(i)
flatjoblist2=[]
for cnt,i in enumerate(flatjoblist):
    flatjoblist2.append((flatjoblist[cnt]['core'], flatjoblist[cnt]['job']))


### CPU lines working !!
CpucoreDic={}
Maxnplst=[]
Maxcorelst = [str(i) for i in range(maxcores)]
UnusedAndDeclaredlst=[]
for i in range(maxnp):
    CpucoreDic['Cpu'+str(i)+'line']=''      # Cpu0line, Cpu1line, Cpu2line='','',''
    Maxnplst.append(str(i))

if len(nodeinitsDic)>1:
    for nodenr, wnpropertieslst in zip(wndicrm.keys(), wndicrm.values()):
        MaxNPlstTmp=Maxnplst[:] # ( ???? )
        MaxcorelstTmp=Maxcorelst[:] # ( ???? )
        if wnpropertieslst == '?':
            for cpuline in CpucoreDic:
                CpucoreDic[cpuline]+='_'
        elif len(wnpropertieslst)==1:
            for cpuline in CpucoreDic:
                CpucoreDic[cpuline]+='_'
        else:
            HAS_JOBS=0
            ownNP=wnpropertieslst[1]
            ownNP=int(ownNP)
            for element in wnpropertieslst:
                if type(element) == tuple:  #everytime there is a job:
                    HAS_JOBS+=1
                    # print 'wndic[nodenr][%r] is tuple' %i
                    core, job = element[0], element[1]
                    CpucoreDic['Cpu'+str(core)+'line']+=str(IdOfUnixAccount[UserOfJobId[job]])
                    MaxNPlstTmp.remove(core)
                    MaxcorelstTmp.remove(core)
                    s = set(MaxcorelstTmp)
                    UnusedAndDeclaredlst = [x for x in MaxNPlstTmp if x not in s]
            
            #print MaxNPlstTmp                 #disabled it 8/7/12
            if HAS_JOBS != ownNP:
                #for core in UnusedAndDeclaredlst:
                #    CpucoreDic['Cpu'+str(core)+'line']+='#'
                #    UnusedAndDeclaredlst.remove(core)
                for core in MaxcorelstTmp:
                    CpucoreDic['Cpu'+str(core)+'line']+='_'
        
            if ownNP < maxnp:
                for core in UnusedAndDeclaredlst:
                    CpucoreDic['Cpu'+str(core)+'line']+='#'
            elif ownNP == maxnp:
                for core in UnusedAndDeclaredlst:
                    CpucoreDic['Cpu'+str(core)+'line']+='_'
            else:
                print 'no SHIT!!'
else:                
    for nodenr, wnpropertieslst in zip(wndic.keys(), wndic.values()):
            MaxNPlstTmp=Maxnplst[:] # ( ???? )
            MaxcorelstTmp=Maxcorelst[:] # ( ???? )
            if wnpropertieslst == '?':
                for cpuline in CpucoreDic:
                    CpucoreDic[cpuline]+='_'
            elif len(wnpropertieslst)==1:
                for cpuline in CpucoreDic:
                    CpucoreDic[cpuline]+='_'
            else:
                HAS_JOBS=0
                ownNP=wnpropertieslst[1]
                ownNP=int(ownNP)
                for element in wnpropertieslst:
                    if type(element) == tuple:  #everytime there is a job:
                        HAS_JOBS+=1
                        core, job = element[0], element[1]
                        CpucoreDic['Cpu'+str(core)+'line']+=str(IdOfUnixAccount[UserOfJobId[job]])
                        MaxNPlstTmp.remove(core)
                        MaxcorelstTmp.remove(core)
                        s = set(MaxcorelstTmp)
                        UnusedAndDeclaredlst = [x for x in MaxNPlstTmp if x not in s]
                
                if HAS_JOBS != ownNP:
                    #for core in UnusedAndDeclaredlst:
                    #    CpucoreDic['Cpu'+str(core)+'line']+='#'
                    #    UnusedAndDeclaredlst.remove(core)
                    for core in MaxcorelstTmp:
                        CpucoreDic['Cpu'+str(core)+'line']+='_'
            
                if ownNP < maxnp:
                    for core in UnusedAndDeclaredlst:
                        CpucoreDic['Cpu'+str(core)+'line']+='#'
                elif ownNP == maxnp:
                    for core in UnusedAndDeclaredlst:
                        CpucoreDic['Cpu'+str(core)+'line']+='_'
                else:
                    print 'no SHIT!!'



# for cnt,state in enumerate(stateafterstr,1):
#     '''
#     For each node, traverse the cores and jobs active, and add the respective Unix IDs to each of the CPUx lines
#     '''
#     if state=='?':
#         for cpuline in CpucoreDic:
#             CpucoreDic[cpuline]+='?'
#     Maxcorelst2=Maxcorelst[:]
#     for core,job in zip(big[cnt]['core'], big[cnt]['job']):
#         '''
#         CpucoreDic['Cpu1line']+='8'
#         '''
#         if core in Maxcorelst2:
#             Maxcorelst2.remove(core)
#         CpucoreDic['Cpu'+str(core)+'line']+=str(IdOfUnixAccount[UserOfJobId[job]])
#         #CpucoreDic['Cpu'+str(unused)+'line']+='_'
        
#     for unused in Maxcorelst2:
#         CpucoreDic['Cpu'+str(unused)+'line']+='_'

CpucoreList=[]
# sorted(d.items(), key=itemgetter(1))
# CpucoreList.sort(CpucoreDic.items(), key=itemgetter(3), reverse=True)
for ind,k in enumerate(CpucoreDic):
    # print CpucoreDic[k]+'=CPU'+str(ind)
    print CpucoreDic['Cpu'+str(ind)+'line'][beginprint:]+'=CPU'+str(ind)



print '\n'
print '===> User accounts and pool mappings <=== ("all" includes those in C and W states, as reported by qstat)'
print 'id |   R +   Q / all |  unix account  | Grid certificate DN (this info is only available under elevated privileges)'

qstatLst.sort(key=lambda unixaccount: unixaccount[1])   # sort by unix account

  
AssIdvalues = IdOfUnixAccount.values()
AssIdkeys = IdOfUnixAccount.keys()
# UserRunningDicValues = UserRunningDic.values()
# UserRunningDickeys = UserRunningDic.keys()
# UserCancelledDicValues = UserCancelledDic.values()
# UserCancelledDickeys = UserCancelledDic.keys()
# UserQueuedDicValues = UserQueuedDic.values()
# UserQueuedDickeys = UserQueuedDic.keys()

#this prints what is actually below the id| R+Q /all | unix account etc line
output=[]
#OLD
# for i in range(len(IdOfUnixAccount)):
#     output.append([AssIdvalues[i], UserRunningDicValues[i], UserQueuedDicValues[i], UserCancelledDicValues[i]+ UserRunningDicValues[i]+ UserQueuedDicValues[i], AssIdkeys[i]])
# ####### workaround, na brw veltistopoiisi

for id in IdOfUnixAccount:
    if id not in UserRunningDic:
        UserRunningDic[id]=0
    if id not in UserQueuedDic:
        UserQueuedDic[id]=0
    if id not in UserCancelledDic:
        UserCancelledDic[id]=0
    if id not in UserWaitingDic:
        UserWaitingDic[id]=0
    if id not in UserEDic:
        UserEDic[id]=0


for id in Usersortedlst:#IdOfUnixAccount:
    output.append([IdOfUnixAccount[id[0]], UserRunningDic[id[0]], UserQueuedDic[id[0]], UserCancelledDic[id[0]]+ UserRunningDic[id[0]]+ UserQueuedDic[id[0]]+ UserWaitingDic[id[0]]+ UserEDic[id[0]], id])
####### workaround, na brw veltistopoiisi
output.sort(key=itemgetter(3), reverse=True)
for line in output:
    print '%2s | %3s + %3s / %3s | %14s |' % (line[0], line[1], line[2], line[3], line[4][0])


os.chdir(homepath+'qtop/qtop')
# print nodeinitsDic
# print remapnr

print 'Thanks for watching!'
#!/usr/bin/env python

################################################
#                                              #
#              qtop v.0.1.6                    #
#                                              #
#     Licensed under MIT-GPL licenses          #
#                                              #
#                     Fotis Georgatos          #
#                     Sotiris Fragkiskos       #
################################################

"""

changelog:
=========
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


import sys,os,glob,re,yaml,datetime

#savedir='~/qtop-input/results'
#savedir=os.path.expanduser(savedir)

YAML_OUTPUT=2 # values: 1, 2, controls the YAML format pbsnodes.yaml uses

outputpath='~sfranky/qtop-input/outputs/' #where the output for each job is stored
outputpath=os.path.expanduser(outputpath)
    

#if not os.path.exists(savedir):
#    cmd='mkdir '+savedir
#    fp = os.popen(cmd)   #execute cmd 'mkdir /home/sfragk/qtop-input/results'

    
def get_state(fin):
    """
    this gets the state of each of the nodes for each given file-job (*.out), appends it to variable status and
    returns the status, which is of the form e.g. ----do---dddd
    """
    state=''
    for line in fin:
        line.strip()
        if line.find('state: ')!=-1:
            nextchar=line.split()[1].strip("'")
            if nextchar=='f': state+='-'
            else:
                state+=nextchar
    fin.close() 
    return state

OnlineNodes=0
OfflineNodes=0
TotalCores=0
QstatQline,QueueName,Mem,CPUtime,Walltime,Node,Run,Queued,Lm,State,TotalRuns,TotalQueues=0,0,0,0,0,0,0,0,0,0,0,0 #for readQstatQ
Jobid,Jobnr,CEname,Name,User,TimeUse,S,Queue=0,0,'','','','','','' #for readQstat
qstatqLst,qstatLst=[],[]
lastnode=0
nonodes=[]

def ReadPbsNodes(fin,fout):
    """
    read pbsnodes.out sequentially and put in respective yaml file
    """
    global OnlineNodes
    global OfflineNodes
    global TotalCores
    global lastnode
    global nonodes
    nodenr='000'
    for line in fin:
        line.strip()
        if re.search('^\w+(\.\w+)+', line)!=None:
            m=re.search('\w+(\.\w+)+', line)
            dname=m.group(0)
            '''
            extract highest node number,online nodes
            '''
            if re.search('^([A-Za-z]+)(\d+)',dname)!=None:
                n=re.search('^([A-Za-z]+)(\d+)',dname)
                #check if there are missing (not installed?) nodes not reported in pbsnodes.out
                # and store them in list nonodes
                if int(nodenr)!=int(n.group(2))-1:
                    nonodes.append(int(nodenr)+1)
                    nodenr=n.group(2)
                else:
                    nodenr=n.group(2)
                OnlineNodes+=1
            #
            yaml.dump({'domainname': dname}, fout, default_flow_style=False)
        elif line.find('state = ')!=-1:
            nextchar=line.split()[2][0]
            if nextchar=='f': 
                state='-'
            elif (nextchar=='d')|(nextchar=='o'):
                state=nextchar
                OfflineNodes+=1
            else:
                state=nextchar
            yaml.dump({'state': state}, fout, default_flow_style=False)
        elif line.find('np = ')!=-1:
            np=line.split()[2][0:]
            yaml.dump({'np': int(np)}, fout, default_flow_style=False)
            TotalCores+=int(np)
        elif line.find('jobs = ')!=-1:
            #yaml.dump({'jobs': ''}, fout, default_flow_style=False)
            ljobs=line.split('=')[1].split(',')
            #print 'ljobs is', ljobs
            joblist=[]
            prev_job=0
            for job in ljobs:
                core=job.strip().split('/')[0]
                job=job.strip().split('/')[1:][0].split('.')[0]
                #variant 1: eg:         {core: '0', job: '646922'}
                if YAML_OUTPUT==1:
                    yaml.dump({'job': job, 'core': core}, fout, default_flow_style=True)
                #variant 2: eg:         - core: '0'
                #                         job: '647568'
                if YAML_OUTPUT==2:
                    joblist.append({'core':core, 'job':job})
                prev_job=job

            if YAML_OUTPUT==2:
                yaml.dump(joblist, fout, default_flow_style=False)

        elif line.find('gpus = ')!=-1:
            gpus=line.split()[2][0:]
            yaml.dump({'gpus': int(gpus)}, fout, default_flow_style=False)
        elif line.startswith('\n'):
            fout.write('\n')
    lastnode=nodenr
    #if lastnode!=OnlineNodes:
    #    print n.group(2)
    ###print lastnode, OnlineNodes, OfflineNodes
    

def ReadQstatQ(fin,fout):
    global QueueName,Mem,CPUtime,Walltime,Node,Run,Queued,Lm,State,TotalRuns,TotalQueues,qstatqLst
    """
    read qstat-q.out sequentially and put in respective yaml file
    """
    Queuesearch='^([a-z]+)\s+(--)\s+(--|\d+:\d+:\d+)\s+(--|\d+:\d+:\d+)\s+(--)\s+(\d+)\s+(\d+)\s+(--|\d+)\s+([DE] R)'
    RunQdSearch='^\s*(\d+)\s+(\d+)'
    for line in fin:
        line.strip()
        # searches for something like: biomed             --      --    72:00:00   --   31   0 --   E R
        if re.search(Queuesearch, line)!=None:
            m=re.search(Queuesearch, line)
            QstatQline,QueueName,Mem,CPUtime,Walltime,Node,Run,Queued,Lm,State=m.group(0),m.group(1),m.group(2),m.group(3),m.group(4),m.group(5),m.group(6),m.group(7),m.group(8),m.group(9)
            qstatqLst.append((QueueName,Run,Queued,Lm,State))
            yaml.dump([{'QueueName': QueueName}, {'Running': int(Run), 'Queued': int(Queued), 'Lm': Lm, 'State': State}], fout, default_flow_style=False)
            fout.write('\n')
        elif re.search(RunQdSearch, line)!=None:
            n=re.search(RunQdSearch, line)
            TotalRuns, TotalQueues=n.group(1), n.group(2)
    fout.write('---\n')
    yaml.dump({'Total Running': int(TotalRuns), 'Total Queued': int(TotalQueues)}, fout, default_flow_style=False )


def ReadQstat(fin,fout):
    global Jobid,Jobnr,CEname,Name,User,TimeUse,S,Queue
    """
    read qstat-q.out sequentially and put in respective yaml file
    """
    UserQueueSearch='^((\d+)\.([A-Za-z]+[0-9]*))\s+([A-Za-z0-9_]+)\s+([A-Za-z]+[0-9]+)\s+(\d+:\d+:\d*|0)\s+([CWRQ])\s+(\w+)'
    RunQdSearch='^\s*(\d+)\s+(\d+)'
    for line in fin:
        line.strip()
        # searches for something like: 422561.cream01             STDIN            see062          48:50:12 R see       
        if re.search(UserQueueSearch, line)!=None:
            m=re.search(UserQueueSearch, line)
            Jobid,Jobnr,CEname,Name,User,TimeUse,S,Queue=m.group(1),m.group(2),m.group(3),m.group(4),m.group(5),m.group(6),m.group(7),m.group(8)
            qstatLst.append([[Jobnr],User,S,Queue])
            fout.write('---\n')
            yaml.dump([{'JobId': Jobid}, {'UnixAccount': User}, {'S': S}, {'Queue': Queue}], fout, default_flow_style=False)
            fout.write('...\n')


with open('/home/ubuntu/qt/pbsnodes.yaml', 'w'):
    pass
with open('/home/ubuntu/qt/qstat-q.yaml', 'w'):
    pass
with open('/home/ubuntu/qt/qstat.yaml', 'w'):
    pass
# this empties the files with every run of the python script

outputDirs=[]
os.chdir(outputpath)
outputDirs+=glob.glob('sfragk*') 
for dir in outputDirs:
    if dir=='sfragk_sDNCrWLMn22KMDBH_jboLQ':  #slight change:just use this dir, don't put *everything* in pbsnodes.yaml !!
        os.chdir(dir)
        yamlstream=open('/home/ubuntu/qt/pbsnodes.yaml', 'a')
        yamlstream2=open('/home/ubuntu/qt/qstat-q.yaml', 'a')
        yamlstream3=open('/home/ubuntu/qt/qstat.yaml', 'a')
        fin=open('pbsnodes.out','r')
        fin2=open('qstat-q.out','r')
        fin3=open('qstat.out','r')
        ReadPbsNodes(fin,yamlstream)
        ReadQstatQ(fin2,yamlstream2)
        ReadQstat(fin3,yamlstream3)
        fin.close()
        fin2.close()
        fin3.close()
        os.chdir('..')

#os.chdir('/home/sfranky/inp/outputs/sfragk_aRk11NE12OEDGvDiX9ExUg/')            
#fin=open('pbsnodes.out','r')            
#ReadPbsNodes(fin,yamlstream)

def write_string(filename,something):
    '''
    appends the contents of a variable to a file 
    '''
    fout=open(filename,'w') 
    fout.write(something)
    fout.close()

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
        #write_string(statefile,getst)
'''

#### read from qstat-q.yaml


#### QTOP  DISPLAY #######################




print 'PBS report tool. Please try: watch -d /home/sfragk/off/qtop . All bugs added by fotis@cern.ch. Cross fingers now...\n'
print '===> Job accounting summary <=== (Rev: 3000 $) %s WORKDIR=to be added\n' % (datetime.datetime.today())
print 'Usage Totals:\t%s/%s\t Nodes | x/%s\t Cores |\t blah blah' %(OnlineNodes-OfflineNodes, OnlineNodes, TotalCores)
#print 'Queues: | '+elem[0]+': '+elem[1]+'+'+elem[2]+' \n' % [elem[0] for elem in qstatqLst], [elem[1] for elem in qstatqLst], [elem[2] for elem in qstatqLst]
print 'Queues: | ',
for i in qstatqLst:
    print i[0]+': '+i[1]+'+'+i[2]+' |',
print '* implies blocked'
print '\n'
print '===> Worker Nodes occupancy <=== (you can read vertically the node IDs; nodes in free state are noted with - )'

#lastnode=169 #for test purposes
u=''
if lastnode<10:
    for i in range(lastnode):
        u+= str(i+1)
    print u+'={__WNID__}'
elif lastnode<100:
    dec=str(lastnode)[0]
    unit=str(lastnode)[1]
    d,u='',''
    for i in range(int(dec)):
        if i==0:
            d+=str(i)*9
        else:
            d+=str(i)*10
    d+=str(i+1)*(int(unit)+1)
    print d+'={__Node__}'
    for i in range(int(str(lastnode)[1])):
        u+=str(i+1)
    print '1234567890'*int(str(lastnode)[0])+u+'={___ID___}'
elif lastnode>99:
    cent=str(lastnode)[0]
    dec=str(lastnode)[1]
    unit=str(lastnode)[2]
    c,d,u='','',''
    for i in range(int(cent)+1):
        if i==0:
            c=str(i)*99
        elif i==1:
            c+=str(i)*10*int(dec)
    c+=str(i)*(int(unit)+1)
    print c+'={_Worker_}'
    d+='0'*9+'1'*10+'2'*10+'3'*10+'4'*10+'5'*10+'6'*10+'7'*10+'8'*10+'9'*10
    if range(int(dec))!=[]:
        for i in range(int(dec)):
            if i==0:
                d+=str(i)*10
            else:
                d+=str(i)*10
        d+=str(i+1)*(int(unit)+1)
    else:
        for i in range(int(unit)+1):
            d+=str(dec)
    print d+'={__Node__}'
    u='1234567890'*9
    for i in range(int(str(lastnode)[2])):
        u+=str(i+1)
    print '1234567890'*(1+int(str(lastnode)[1]))+u+'={___ID___}'

#if OnlineNodes!=lastnode:

yamlstream=open('/home/ubuntu/qt/pbsnodes.yaml', 'r')
statebefore=get_state(yamlstream)
stateafter=statebefore[:nonodes[0]-1]+'?'+statebefore[nonodes[0]-1:]
for i in range(1,len(nonodes)):
    stateafter=stateafter[:nonodes[i]-1]+'?'+stateafter[nonodes[i]-1:]
print stateafter+'=Node state'
print '_'*int(lastnode)+'=CPU0'
print '_'*int(lastnode)+'=CPU1'
print '\n'
print '===> User accounts and pool mappings <=== (''all'' includes those in C and W states, as reported by qstat)'
print 'id |  R +  Q / all|  unix account  | Grid certificate DN (this info is only available under elevated privileges)'

qstatLst.sort(key=lambda unixaccount: unixaccount[1])   # sort by unix account

#for i in range(1,len(qstatLst)-2):
#    if qstatLst[0][1]==qstatLst[0+i][1]:
#        qstatLst[0][0].extend(qstatLst[0+i][0])
#print qstatLst


#yamlstream3=open('/home/ubuntu/qt/qstat2.yaml', 'w')
#yaml.dump(qstatLst, yamlstream3, default_flow_style=False)


JobIds=[]
UnixAccounts=[]
Ss=[]
Queues=[]
userset=set()
finr=open('/home/ubuntu/qt/qstat.yaml', 'r')
for line in finr:
    if line.startswith('- JobId:'):
        JobIds.append(line.split()[2].split('.')[0])
    elif line.startswith('- UnixAccount:'):
        UnixAccounts.append(line.split()[2])
    elif line.startswith('- S:'):
        Ss.append(line.split()[2])
    elif line.startswith('- Queue:'):
        Queues.append(line.split()[2])
#print 'JobIds is', JobIds, '\n'
#print 'UnixAccounts is', UnixAccounts, '\n'
#print 'Ss is', Ss, '\n'
#print 'Queues is', Queues, '\n'
finr.close()


#solution for counting R,Q,C attached to each user
UserRunningDic, UserQueuedDic, UserCancelledDic = {}, {}, {}

for user,status in zip(UnixAccounts,Ss):
    if status=='R':
        UserRunningDic[user] = UserRunningDic.get(user, 0) + 1
    elif status=='Q':
        UserQueuedDic[user] = UserQueuedDic.get(user, 0) + 1
    elif status=='C':
        UserCancelledDic[user] = UserCancelledDic.get(user, 0) + 1

for k in UserRunningDic:
    UserQueuedDic.setdefault(k, 0)
    UserCancelledDic.setdefault(k, 0)

AssignedIdDic = {}
j=0
for i in UserRunningDic:
    AssignedIdDic[i]=j
    j+=1

AssIdvalues = AssignedIdDic.values()
AssIdkeys = AssignedIdDic.keys()
UserRunningDicValues = UserRunningDic.values()
UserRunningDickeys = UserRunningDic.keys()
UserCancelledDicValues = UserCancelledDic.values()
UserCancelledDickeys = UserCancelledDic.keys()
UserQueuedDicValues = UserQueuedDic.values()
UserQueuedDickeys = UserQueuedDic.keys()

for i in range(len(AssignedIdDic)):
    print '%2s | %2s + %2s / %2s | %s' % (AssIdvalues[i], UserRunningDicValues[i], UserQueuedDicValues[i], UserCancelledDicValues[i]+ UserRunningDicValues[i]+ UserQueuedDicValues[i], AssIdkeys[i])

print 'AssIdvalues are ', AssIdvalues
print 'AssIdkeys are ', AssIdkeys
print 'AssignedIdDic is ', AssignedIdDic

print 'UserRunningDic is ', UserRunningDic
print 'UserQueuedDic is ', UserQueuedDic
print 'UserCancelledDic is ', UserCancelledDic

#qStatGrandlist=[]
#for lista in yaml.load_all(open('/home/ubuntu/qt/qstat.yaml')):
#    qStatGrandlist.append(lista)
#
#print '\n\n'
#print qStatGrandlist[0]
#print qStatGrandlist[0][0]
#print qStatGrandlist[0][0]['JobId']
#print qStatGrandlist[0][1]['UnixAccount']

#AllJobsDic = {}
#Ssdic2 = {}
#for LineLst in qStatGrandlist:
#    #print LineLst
#    AllJobsDic[LineLst[1]['UnixAccount']] = accountdic.get(LineLst[1]['UnixAccount'], 0) + 1
#    #Ssdic2[LineLst[2]['S']] = accountdic.get(LineLst[1]['S'], 0) + 1
#print AllJobsDic
##print Ssdic2
##print Ssdic2



#qStatGrandlist.sort(key=lambda unixaccount: unixaccount[1])
#print qStatGrandlist

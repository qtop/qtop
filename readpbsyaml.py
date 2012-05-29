big=[]
counter=0 
''' 
this is the actual counter for the machines. If there are 3 machines names wn001,wn004,wn121,
then counter will count 3. Qtop displays the WN ID, which will be 001,002,003.
This will also extract the max number of cores it should display.
'''
fin=open('/home/sfranky/qtop/qtop/pbsnodes.yaml','r')

maxcores=0

for line in fin:
	line.strip()
	if line.startswith('domainname:'):
		counter+=1
		wnstr=line.split()[1].split('.')[0][2:]
		wnint=int(line.split()[1].split('.')[0][2:]) 
		d={}
		d['machine nr %r' % counter]=[wnint]
		d['core']=[]
		d['job']=[]
	elif line.startswith('state:'):
		state=line.split()[1].strip("'")
	elif line.startswith('np'):
		np=line.split()[1]
	elif line.startswith('- core'):
		core=line.split()[2].strip("'")
		if int(core)>int(maxcores):
			maxcores=int(core)
		d['core'].append(core)
	elif line.startswith('  job:'):
		jobid=line.split()[1].strip("'")
		d['job'].append(jobid)
	elif line.startswith('gpus'):
		gpus=line.split()[1]
	elif line=='\n':
		big.append(d)
		del d

fin.close()
maxcores+=1

#for bigelement in big:
#	# bigelement['core'].extend(map(lambda a:str(a), range(maxcores)))
#	alllist=bigelement['core']+ map(lambda a:str(a), range(maxcores))
#	alllist=list(set(alllist))
#	alllist.sort()
#	bigelement['core'].extend(alllist)
#



#print 'the highest core nr is: ', maxcores
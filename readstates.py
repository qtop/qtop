#!/usr/bin/python

################################################
#                                              #
#              qtop v.0.1.1                    #
#                                              #
#                                              #
#                     Fotis Georgatos, ????    #
#                     Sotiris Fragkiskos, CERN #
################################################

"""

changelog:
=========


0.1.1: changed implementation in get_state()

0.1.0: just read a pbsnodes-a output file and gather the results in a single line


"""


import sys



"""

def write_state(fout):
    with open('fout', mode='a') as file:
        string=status
        file.write(string)
"""



def get_state(fin):
    status=''
    for line in fin:
        line.strip()
        if line.find('state = ')!=-1:
            nextchar=line.split()[2][0]
            if nextchar=='f': status+='-'
            else:
                status+=nextchar
    return status


if __name__ == "__main__":

    fin=open(sys.argv[1], "r")

    print get_state(fin)

    fin.close()



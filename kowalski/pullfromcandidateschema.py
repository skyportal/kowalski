import sys

def pull(filename):

    f=open(filename,'r')
    keys=[]
    for line in f.readlines():
        if '{"name"' in line:
#            print(line)
#            None
            splt=line.split(',')
            splt2=splt[0].split(':')
#            print(splt2[1])
            x=splt2[1]
            x2=x.replace('"','')
            x3=x2.replace(' ','')
            keys.append(x3)
#        else:
#            print(line)
    f.close()
    print(keys)
if __name__ == '__main__':
    pull(sys.argv[1])

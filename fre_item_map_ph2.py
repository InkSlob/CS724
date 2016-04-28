#!/usr/bin/env python
import sys
sys.path.append('.')
a=[]
val=[]
for  i in sys.stdin:
        i = i.strip()
        items = i.split()
        a.append(items)
a = map(set,a)
for line in open('valA','r'):
    new_line = line.strip()
    if new_line:
            new_line=new_line.split()
            val.append(new_line)
val = map(frozenset,val)
for i in a:
    for j in val:
            if j.issubset(i):
                    x,y=j
                    print "(%s %s),1"%(x,y)

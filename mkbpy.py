from scipy import *
from scipy.linalg import *
import numpy as np
import sys
import csv

filename = 'baskets.txt'

item_cnt = zeros(200)

with open(filename, 'r') as f:
  basket = f.read()
  basket = basket.splitlines()

na = size(basket)
cnt = 0

for j in range(na):
  line = basket[j]
  line = line.split(',')
  nb = size(line)
  for x in range(nb):
	element = line[x]
	item_cnt[cnt] = element
	cnt = cnt + 1
	
item_set = np.unique(item_cnt)
print item_set




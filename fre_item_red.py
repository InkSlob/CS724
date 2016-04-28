#!/usr/bin/env python
import sys
pre_pair = None
for pair in sys.stdin:
	if pair!=pre_pair:
		print pair
		pre_pair= pair
		

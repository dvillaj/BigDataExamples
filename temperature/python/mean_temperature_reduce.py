#!/usr/bin/env python

import sys

(last_key, sum_val, num_elementos) = (None, 0.0, 0)
for line in sys.stdin:
  (key, val) = line.strip().split("\t")
  # print("line=%s, key = %s, last_key = %s, val = %s, sum_val = %d, num_elementos = %d" %
  #     (line.strip(), key, last_key, val, sum_val, num_elementos))
  if last_key and last_key != key:
    print "%s\t%s" % (last_key, sum_val / num_elementos)
    num_elementos = 1
    (last_key, sum_val) = (key, float(val))
  else:
    num_elementos = num_elementos + 1
    (last_key, sum_val) = (key, sum_val + float(val))

if last_key:
  print "%s\t%10.2f" % (last_key, sum_val / num_elementos)
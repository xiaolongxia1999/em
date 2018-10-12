import numpy as np
import random

#形参，默认实参
def gen_ran(seed=11):
    a = 0
    #实参
    if seed == 11:
        seed = 100
        a = random.random()*100
    else:
        a = 1000000+ random.random()*100
    return a

b = gen_ran()
print(b)

from random import *

n = randint(1, 100000) + randint(1000000, 10000000)
print("Generating", n, "random numbers for the test")
sum = 0.0
f = open(("test.dat"), "w")
while n > 0:
	x = random()
	f.write(str(x))
	f.write("\n")
	sum += x
	n -= 1
f.close()
print("Data set is saved in file 'test.dat'.")
print("The sum should be", sum)
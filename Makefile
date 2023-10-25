run:
	g++ run.cc -pthread
test:
	g++ run.cc -pthread
	numactl --cpunodebind=0 --membind=0 ./a.out 1
test-base:
	g++ run.cc -pthread
	numactl --cpunodebind=0 --membind=0 ./a.out 1
test-mmap:
	g++ -DMMAP run.cc -pthread
	numactl --cpunodebind=0 --membind=0 ./a.out 1
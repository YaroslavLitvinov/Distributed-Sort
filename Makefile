
all:
	gcc -o sort_merge sort.c main.c -I . -std=c99 -g -lzmq


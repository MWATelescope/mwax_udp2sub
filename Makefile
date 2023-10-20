CC=gcc
CFLAGS=-Wall -O3 -g -Isrc -march=native -lm -lrt -lcfitsio -c
SRCS=src/mwax_udp2sub.c src/common.c src/delaygen.c src/heartbeat.c src/makesub.c src/metafits.c src/parse.c src/receive.c

#all:
#	$(CC) $(SRCS)  $(CFLAGS) -o mwax_udp2sub

config.o: src/config.c
	$(CC) $(CFLAGS) src/config.c -o config.o

common.o: src/common.c
	$(CC) $(CFLAGS) src/common.c -o common.o	

receive.o: src/receive.c
	$(CC) $(CFLAGS) src/receive.c -o receive.o

parse.o: src/parse.c
	$(CC) $(CFLAGS) src/parse.c -o parse.o

metafits.o: src/metafits.c
	$(CC) $(CFLAGS) src/metafits.c -o metafits.o
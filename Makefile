all:
	gcc src/mwax_udp2sub.c -Isrc -g -omwax_u2s -lpthread -Ofast -march=native -lm -lrt -lcfitsio -Wall

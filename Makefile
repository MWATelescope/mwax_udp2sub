all:
	gcc src/mwax_udp2sub.c -g -omwax_u2s -lpthread -Ofast -march=native -lrt -lcfitsio -Wall

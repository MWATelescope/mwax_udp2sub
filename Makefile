all:
	gcc src/mwax_udp2sub.c -Isrc -omwax_u2s -lpthread -march=native -lm -lrt -lcfitsio -Wall

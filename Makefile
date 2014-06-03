# Makefile for OS Project 5: MyCachingFileSystem

TAR = ex5.tar
TAR_CMD = tar cvf
CC = g++ -std=c++11 -Wall `pkg-config fuse --cflags --libs`

all: MyCachingFileSystem

MyCachingFileSystem: MyCachingFileSystem.cpp
	$(CC) MyCachingFileSystem.cpp -o MyCachingFileSystem

clean:
	rm -f $(TAR) MyCachingFileSystem

tar: MyCachingFileSystem.cpp Makefile README
	$(TAR_CMD) $(TAR) MyCachingFileSystem.cpp Makefile README
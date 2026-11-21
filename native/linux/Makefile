CC := gcc
CFLAGS := -std=c11 -O2 -Wall -Wextra -D_GNU_SOURCE
TARGET := ../build/sparecycle-process-manager

.PHONY: all clean

all: $(TARGET)

$(TARGET): process_manager.c ../include/process_manager.h
	mkdir -p ../build
	$(CC) $(CFLAGS) process_manager.c -o $(TARGET)

clean:
	rm -f $(TARGET)

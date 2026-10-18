# Makefile - telemetry_logger
# Target: Raspberry Pi Zero W (ARMv6) running Raspberry Pi OS (Bullseye / Bookworm)
# Also builds on any Linux x86-64 for development/testing.
#
# Dependencies (install once):
#   sudo apt-get install -y libwebsockets-dev libcjson-dev
#
# Usage:
#   make              - build
#   make run          - build and run (logs to metrics_log.txt)
#   make clean        - remove binary and log
#   make deps         - install apt dependencies

CC      := gcc
TARGET  := telemetry_logger
SRC     := telemetry_logger.c

#Use pkg-config when available; fall back to plain -l flags
PKG_LWS := $(shell pkg-config --exists libwebsockets 2>/dev/null && echo yes)

ifeq ($(PKG_LWS),yes)
    LWS_CFLAGS  := $(shell pkg-config --cflags libwebsockets)
    LWS_LDFLAGS := $(shell pkg-config --libs   libwebsockets)
else
    LWS_CFLAGS  :=
    LWS_LDFLAGS := -lwebsockets
endif

CFLAGS  := -Wall -Wextra -O2 -g \
           -D_GNU_SOURCE \
           -pthread \
           $(LWS_CFLAGS)

LDFLAGS := -pthread \
           $(LWS_LDFLAGS) \
           -lcjson \
           -lm

.PHONY: all run clean deps deps-python

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

run: $(TARGET)
	./$(TARGET)

clean:
	rm -f $(TARGET) metrics_log.txt

#C build dependencies  run this on the Pi.
deps:
	sudo apt-get update
	sudo apt-get install -y libwebsockets-dev libcjson-dev

#Python deps for probe_jetstream.py / analyze_metrics.py  run this on
#whichever machine does the probing/analysis (not necessarily the Pi).
deps-python:
	pip3 install --break-system-packages websockets numpy pandas matplotlib scipy

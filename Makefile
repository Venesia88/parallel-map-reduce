CC = mpicc
CXX = mpicxx
CXXFLAGS = -O3 -lm -pthread
CFLAGS = -O3 -lm -pthread
TARGETS = hw4

.PHONY: all
all: $(TARGETS)

.PHONY: clean
clean:
	rm -f $(TARGETS)
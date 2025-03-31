CXXFLAGS=-I. -std=c++17 -pthread
LDFLAGS=-lcurl
LD=g++
CC=g++

all: main graph_crawler

main: main.o
        $(LD) $< -o $@ $(LDFLAGS)

graph_crawler: main.cpp
        $(CC) $(CXXFLAGS) -o graph_crawler main.cpp $(LDFLAGS)

main.o: main.cpp
        $(CC) $(CXXFLAGS) -c $< -o $@

clean:
        -rm main main.o graph_crawler

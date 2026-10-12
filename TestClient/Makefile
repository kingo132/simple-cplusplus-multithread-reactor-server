CXX ?= g++
CXXFLAGS ?= -O2
override CXXFLAGS += -std=c++11 -pthread -Wall -Wextra
TARGET = tcp_test_client

all: $(TARGET)

$(TARGET): main.cpp
	$(CXX) $(CXXFLAGS) -o $(TARGET) main.cpp

clean:
	rm -f $(TARGET) *.o

.PHONY: all clean

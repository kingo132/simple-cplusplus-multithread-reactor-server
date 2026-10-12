# Build the test plugin: libtesthandler.so
CXX ?= g++
CXXFLAGS ?= -O2
override CXXFLAGS += -std=c++11 -Wall -Wextra -g -fPIC -I../MultithreadServer
override LDFLAGS += -shared

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S), Darwin)
    override LDFLAGS += -undefined dynamic_lookup   # srv_* symbols come from the server executable
endif
ifneq ($(SANITIZE),)
    override CXXFLAGS += -fsanitize=$(SANITIZE) -fno-omit-frame-pointer
    override LDFLAGS += -fsanitize=$(SANITIZE)
endif

TARGET = libtesthandler.so

all: $(TARGET)

$(TARGET): dll_interface.cpp ../MultithreadServer/server_api.h
	$(CXX) $(CXXFLAGS) -o $@ dll_interface.cpp $(LDFLAGS)

clean:
	rm -f $(TARGET)

.PHONY: all clean

# Build the test plugin: libtesthandler.so
CXX ?= g++
CXXFLAGS ?= -O2
CXXFLAGS += -std=c++11 -Wall -Wextra -g -fPIC -I../MultithreadServer
LDFLAGS += -shared

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S), Darwin)
    LDFLAGS += -undefined dynamic_lookup   # srv_* symbols come from the server executable
endif
ifneq ($(SANITIZE),)
    CXXFLAGS += -fsanitize=$(SANITIZE) -fno-omit-frame-pointer
    LDFLAGS += -fsanitize=$(SANITIZE)
endif

TARGET = libtesthandler.so

all: $(TARGET)

$(TARGET): dll_interface.cpp ../MultithreadServer/server_api.h
	$(CXX) $(CXXFLAGS) -o $@ dll_interface.cpp $(LDFLAGS)

clean:
	rm -f $(TARGET)

.PHONY: all clean

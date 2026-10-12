# Build the server engine (mulserver).
#   make            release build
#   make DEBUG=1    debug build with -O0 -g
#   make SANITIZE=address|thread|undefined
CXX ?= g++
CXXFLAGS ?= -O2
CXXFLAGS += -std=c++11 -Wall -Wextra -g -pthread -DSRV_ENGINE_INTERNAL -I.
LDFLAGS += -pthread

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S), Linux)
    LDFLAGS += -ldl -rdynamic      # export srv_* symbols to the plugin
else ifeq ($(UNAME_S), Darwin)
    LDFLAGS += -Wl,-export_dynamic
endif

ifeq ($(DEBUG), 1)
    CXXFLAGS += -O0
endif
ifneq ($(SANITIZE),)
    CXXFLAGS += -fsanitize=$(SANITIZE) -fno-omit-frame-pointer
    LDFLAGS += -fsanitize=$(SANITIZE)
endif

SRCS = main.cpp server.cpp server_api.cpp connection_manager.cpp ring_queue.cpp \
       protocol_handler.cpp tcp_handler.cpp udp_handler.cpp \
       event_dispatcher_factory.cpp poll_dispatcher.cpp epoll_dispatcher.cpp \
       configuration_manager.cpp daemon_manager.cpp dll_functions.cpp log_manager.cpp utility.cpp

OBJS = $(SRCS:.cpp=.o)
DEPS = $(SRCS:.cpp=.d)

# Target executable
TARGET = mulserver

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -MMD -MP -c $< -o $@

clean:
	rm -f $(OBJS) $(DEPS) $(TARGET)

-include $(DEPS)

.PHONY: all clean

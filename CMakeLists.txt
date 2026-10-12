# CMake build for the reactor server engine.
#   cmake -B build && cmake --build build -j
#   cmake -G Xcode -B build-xcode        # Xcode project on macOS
#
# Targets:
#   mulserver        the server engine (MultithreadServer/*.cpp)
#   libtesthandler   the example / test plugin (TestHandler/dll_interface.cpp)
#   tcp_test_client  load + correctness test client (TestClient/main.cpp)
#   ring_queue_test  unit test of the ring queue (tests/ring_queue_test.cpp)
# The Makefiles in MultithreadServer/, TestHandler/ and TestClient/ build the
# same things without CMake (used by tests/run_tests.sh).
#
# Other projects can add_subdirectory() this directory and link their plugin
# against the `reactor_api` interface target (see reactor_add_plugin()).
cmake_minimum_required(VERSION 3.10)
project(MultithreadReactorServer CXX)

# The code is written in C++11; a parent project may ask for a newer standard
if(NOT CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 11)
endif()
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# Single-config generators (Makefiles, Ninja) default to an optimised build
# with debug symbols; multi-config generators (Xcode, VS) choose per build.
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

# std::thread needs the platform thread library; prefer the -pthread flag
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

set(REACTOR_DIR ${CMAKE_CURRENT_SOURCE_DIR}/MultithreadServer)

# The server executable: core (server, queues, connections), protocol
# handlers, event dispatchers and the process level helpers
add_executable(mulserver
    ${REACTOR_DIR}/main.cpp
    ${REACTOR_DIR}/server.cpp
    ${REACTOR_DIR}/server_api.cpp
    ${REACTOR_DIR}/connection_manager.cpp
    ${REACTOR_DIR}/ring_queue.cpp
    ${REACTOR_DIR}/protocol_handler.cpp
    ${REACTOR_DIR}/tcp_handler.cpp
    ${REACTOR_DIR}/udp_handler.cpp
    ${REACTOR_DIR}/event_dispatcher_factory.cpp
    ${REACTOR_DIR}/poll_dispatcher.cpp
    ${REACTOR_DIR}/epoll_dispatcher.cpp
    ${REACTOR_DIR}/configuration_manager.cpp
    ${REACTOR_DIR}/daemon_manager.cpp
    ${REACTOR_DIR}/dll_functions.cpp
    ${REACTOR_DIR}/log_manager.cpp
    ${REACTOR_DIR}/utility.cpp
)
# SRV_ENGINE_INTERNAL: inside the engine the LOG_* macros come from
# log_manager.h (direct LogManager calls), not from server_api.h (srv_log)
target_compile_definitions(mulserver PRIVATE SRV_ENGINE_INTERNAL)
target_include_directories(mulserver PRIVATE ${REACTOR_DIR})
# CMAKE_DL_LIBS: the library that provides dlopen()/dlsym() (libdl on older glibc)
target_link_libraries(mulserver PRIVATE Threads::Threads ${CMAKE_DL_LIBS})
# Export the srv_* functions to the dynamically loaded plugin (-rdynamic)
# Without it the executable's symbols are not in its dynamic symbol table, and
# dlopen() of a plugin that calls srv_send() etc. fails with "undefined symbol".
set_target_properties(mulserver PROPERTIES ENABLE_EXPORTS ON)

# Header-only target for plugins
# (just the include path of server_api.h; plugins never link the engine code)
add_library(reactor_api INTERFACE)
target_include_directories(reactor_api INTERFACE ${REACTOR_DIR})

# reactor_add_plugin(<name> <sources...>): build a plugin shared library
function(reactor_add_plugin name)
    # MODULE: a library that is only loaded at runtime with dlopen(), never linked against
    add_library(${name} MODULE ${ARGN})
    target_link_libraries(${name} PRIVATE reactor_api)
    # Always lib<name>.so, the name the configs use (dll_file), on every platform
    set_target_properties(${name} PROPERTIES PREFIX "lib" SUFFIX ".so")
    if(APPLE)
        # srv_* symbols are resolved from the server executable at load time
        # (on Linux undefined symbols in a shared library are allowed by default;
        # the macOS linker needs this flag to accept them)
        target_link_options(${name} PRIVATE -undefined dynamic_lookup)
    endif()
endfunction()

# Tests are built by default only when this is the top level project, not
# when another project pulls the engine in with add_subdirectory()
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    option(REACTOR_BUILD_TESTS "Build the test plugin and test client" ON)
else()
    option(REACTOR_BUILD_TESTS "Build the test plugin and test client" OFF)
endif()

if(REACTOR_BUILD_TESTS)
    # Example plugin, built exactly like a user plugin would be
    reactor_add_plugin(testhandler TestHandler/dll_interface.cpp)
    add_executable(tcp_test_client TestClient/main.cpp)
    target_link_libraries(tcp_test_client PRIVATE Threads::Threads)
    # The unit test compiles only the engine files the ring queue depends on
    add_executable(ring_queue_test tests/ring_queue_test.cpp ${REACTOR_DIR}/ring_queue.cpp
                   ${REACTOR_DIR}/log_manager.cpp ${REACTOR_DIR}/utility.cpp)
    target_compile_definitions(ring_queue_test PRIVATE SRV_ENGINE_INTERNAL)
    target_include_directories(ring_queue_test PRIVATE ${REACTOR_DIR})
    target_link_libraries(ring_queue_test PRIVATE Threads::Threads)
endif()

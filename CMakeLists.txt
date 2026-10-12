# CMake build for the reactor server engine.
#   cmake -B build && cmake --build build -j
#   cmake -G Xcode -B build-xcode        # Xcode project on macOS
#
# Other projects can add_subdirectory() this directory and link their plugin
# against the `reactor_api` interface target (see reactor_add_plugin()).
cmake_minimum_required(VERSION 3.10)
project(MultithreadReactorServer CXX)

if(NOT CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 11)
endif()
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

set(REACTOR_DIR ${CMAKE_CURRENT_SOURCE_DIR}/MultithreadServer)

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
target_compile_definitions(mulserver PRIVATE SRV_ENGINE_INTERNAL)
target_include_directories(mulserver PRIVATE ${REACTOR_DIR})
target_link_libraries(mulserver PRIVATE Threads::Threads ${CMAKE_DL_LIBS})
# Export the srv_* functions to the dynamically loaded plugin (-rdynamic)
set_target_properties(mulserver PROPERTIES ENABLE_EXPORTS ON)

# Header-only target for plugins
add_library(reactor_api INTERFACE)
target_include_directories(reactor_api INTERFACE ${REACTOR_DIR})

# reactor_add_plugin(<name> <sources...>): build a plugin shared library
function(reactor_add_plugin name)
    add_library(${name} MODULE ${ARGN})
    target_link_libraries(${name} PRIVATE reactor_api)
    set_target_properties(${name} PROPERTIES PREFIX "lib" SUFFIX ".so")
    if(APPLE)
        # srv_* symbols are resolved from the server executable at load time
        target_link_options(${name} PRIVATE -undefined dynamic_lookup)
    endif()
endfunction()

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    option(REACTOR_BUILD_TESTS "Build the test plugin and test client" ON)
else()
    option(REACTOR_BUILD_TESTS "Build the test plugin and test client" OFF)
endif()

if(REACTOR_BUILD_TESTS)
    reactor_add_plugin(testhandler TestHandler/dll_interface.cpp)
    add_executable(tcp_test_client TestClient/main.cpp)
    target_link_libraries(tcp_test_client PRIVATE Threads::Threads)
    add_executable(ring_queue_test tests/ring_queue_test.cpp ${REACTOR_DIR}/ring_queue.cpp
                   ${REACTOR_DIR}/log_manager.cpp ${REACTOR_DIR}/utility.cpp)
    target_compile_definitions(ring_queue_test PRIVATE SRV_ENGINE_INTERNAL)
    target_include_directories(ring_queue_test PRIVATE ${REACTOR_DIR})
    target_link_libraries(ring_queue_test PRIVATE Threads::Threads)
endif()

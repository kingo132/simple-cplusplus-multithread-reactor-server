/**
 * @file dll_functions.h
 * @brief Loading the business plugin (a shared library) and the table of
 *        its callback function pointers.
 *
 * How the plugin mechanism works:
 *   - The plugin is a .so built from C/C++ code that implements some of
 *     the handle_* functions declared in server_api.h (with C linkage, so
 *     the symbol names are not mangled).
 *   - At start-up main() calls load_dll_functions(): dlopen() loads the
 *     library and dlsym() looks every callback up BY NAME. A missing
 *     symbol is simply a nullptr = "not implemented"; the server checks for
 *     nullptr before every call. Only handle_message_from_client is required.
 *   - The other direction (the plugin calling srv_send() etc.) works
 *     because the server executable is linked with -rdynamic (CMake:
 *     ENABLE_EXPORTS), which puts its srv_* functions into the dynamic
 *     symbol table where the dynamic loader can resolve the plugin's
 *     undefined references against them.
 */
#ifndef DLL_FUNCTIONS_H
#define DLL_FUNCTIONS_H

#include "server_api.h"

/** Every member except `handle` is nullptr when the plugin does not export that function. */
typedef struct dll_func_struct {
    void* handle;  // dlopen() handle of the plugin, for dlclose()
    int (*handle_init)(int argc, char** argv, int thread_type);              // once per thread at start
    void (*handle_fini)(int thread_type);                                    // once per thread at stop
    int (*handle_input_from_client)(const char* data, int len, const SocketInfo* si); // framing, CONN thread
    int (*handle_message_from_client)(const char* data, int len, char** send_data, int* send_len, const SocketInfo* si); // required
    int (*handle_client_open)(char** send_data, int* send_len, const SocketInfo* si); // TCP client accepted
    int (*handle_client_close)(const SocketInfo* si);                        // TCP client gone
    int (*handle_input_from_server)(const char* data, int len, const SocketInfo* si); // framing of outbound streams, CONN thread
    int (*handle_message_from_server)(const char* data, int len, const SocketInfo* si); // package from an outbound connection
    int (*handle_server_open)(const SocketInfo* si, int error);              // srv_connect() result
    int (*handle_server_close)(const SocketInfo* si);                        // outbound connection gone
    int (*handle_write_blocked)(const SocketInfo* si, int blocked);          // send buffer watermarks
    int (*handle_timer)(int* milliseconds);                                  // periodic, every worker
} dll_func_t;

/**
 * dlopen() the plugin and resolve all callbacks (each lookup is logged).
 * @param dll_functions table to fill (zeroed first)
 * @param dll_path      path of the .so; a name without '/' is searched in
 *                      the library path, so use "./libx.so" for the current directory
 * @return false if the library cannot be loaded or lacks
 *         handle_message_from_client (the table is then zeroed again).
 */
bool load_dll_functions(dll_func_t* dll_functions, const char* dll_path);

/** dlclose() the plugin and zero the table. Not used at normal exit (see main.cpp). */
void unload_dll_functions(dll_func_t* dll_functions);

#endif // DLL_FUNCTIONS_H

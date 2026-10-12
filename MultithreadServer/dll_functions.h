#ifndef DLL_FUNCTIONS_H
#define DLL_FUNCTIONS_H

#include "server_api.h"

// Structure for holding the plugin's callback pointers (see server_api.h)
typedef struct dll_func_struct {
    void* handle;
    int (*handle_init)(int argc, char** argv, int thread_type);
    void (*handle_fini)(int thread_type);
    int (*handle_input_from_client)(const char* data, int len, const SocketInfo* si);
    int (*handle_message_from_client)(const char* data, int len, char** send_data, int* send_len, const SocketInfo* si);
    int (*handle_client_open)(char** send_data, int* send_len, const SocketInfo* si);
    int (*handle_client_close)(const SocketInfo* si);
    int (*handle_input_from_server)(const char* data, int len, const SocketInfo* si);
    int (*handle_message_from_server)(const char* data, int len, const SocketInfo* si);
    int (*handle_server_open)(const SocketInfo* si, int error);
    int (*handle_server_close)(const SocketInfo* si);
    int (*handle_write_blocked)(const SocketInfo* si, int blocked);
    int (*handle_timer)(int* milliseconds);
} dll_func_t;

// Loads the shared library and assigns function pointers
bool load_dll_functions(dll_func_t* dll_functions, const char* dll_path);

// Unloads the shared library
void unload_dll_functions(dll_func_t* dll_functions);

#endif // DLL_FUNCTIONS_H

#include "dll_functions.h"

#include <cstring>
#include <dlfcn.h>

#include "log_manager.h"

#define LOAD_FUNCTION(handle, func, symbol)                                         \
    do {                                                                            \
        func = reinterpret_cast<decltype(func)>(dlsym(handle, symbol));             \
        LOG_INFO("Plugin symbol %-28s %s", symbol, func ? "found" : "not implemented"); \
    } while (0)

bool load_dll_functions(dll_func_t* dll_functions, const char* dll_path) {
    std::memset(dll_functions, 0, sizeof(*dll_functions));

    // Load the shared library
    dll_functions->handle = dlopen(dll_path, RTLD_NOW | RTLD_GLOBAL);
    if (!dll_functions->handle) {
        LOG_ERR("Failed to load plugin %s: %s", dll_path, dlerror());
        return false;
    }

    // Load each function
    LOAD_FUNCTION(dll_functions->handle, dll_functions->handle_init, "handle_init");
    LOAD_FUNCTION(dll_functions->handle, dll_functions->handle_fini, "handle_fini");
    LOAD_FUNCTION(dll_functions->handle, dll_functions->handle_input_from_client, "handle_input_from_client");
    LOAD_FUNCTION(dll_functions->handle, dll_functions->handle_message_from_client, "handle_message_from_client");
    LOAD_FUNCTION(dll_functions->handle, dll_functions->handle_client_open, "handle_client_open");
    LOAD_FUNCTION(dll_functions->handle, dll_functions->handle_client_close, "handle_client_close");
    LOAD_FUNCTION(dll_functions->handle, dll_functions->handle_input_from_server, "handle_input_from_server");
    LOAD_FUNCTION(dll_functions->handle, dll_functions->handle_message_from_server, "handle_message_from_server");
    LOAD_FUNCTION(dll_functions->handle, dll_functions->handle_server_open, "handle_server_open");
    LOAD_FUNCTION(dll_functions->handle, dll_functions->handle_server_close, "handle_server_close");
    LOAD_FUNCTION(dll_functions->handle, dll_functions->handle_write_blocked, "handle_write_blocked");
    LOAD_FUNCTION(dll_functions->handle, dll_functions->handle_timer, "handle_timer");

    // Check for mandatory interfaces
    if (!dll_functions->handle_message_from_client) {
        LOG_ERR("handle_message_from_client not implemented!");
        unload_dll_functions(dll_functions);
        return false;
    }
    if (!dll_functions->handle_input_from_client) {
        LOG_WARN("handle_input_from_client not implemented, every read is treated as one package");
    }

    return true;
}

// Function to unload the shared library
void unload_dll_functions(dll_func_t* dll_functions) {
    if (dll_functions->handle) {
        dlclose(dll_functions->handle);
    }
    std::memset(dll_functions, 0, sizeof(*dll_functions));
}

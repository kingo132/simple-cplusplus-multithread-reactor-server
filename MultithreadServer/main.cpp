/**
 * @file main.cpp
 * @brief Entry point of mulserver: configuration, daemon mode, logging,
 *        plugin loading, server start and the signal driven main loop.
 *
 * Usage: mulserver [config_file] [plugin.so]
 *
 * Start-up order (each step needs the previous ones):
 *   1. load the configuration      (everything else is configured by it)
 *   2. install signal handlers     (before any thread exists)
 *   3. daemonize if run_mode = background (fork() must happen before
 *      threads are created, and before files are opened by the logger)
 *   4. create the logger           (from now on LOG_* works)
 *   5. write the pid file          (after the fork: the daemon's pid)
 *   6. load the plugin, run its handle_init(MAIN)
 *   7. Server::start(): sockets, queues, network + worker threads
 *   8. main thread sleeps until SIGINT / SIGTERM, reopens the log on SIGHUP
 * Shutdown runs in reverse order.
 *
 * The main thread itself does no network work; see server.h for the
 * network / worker thread design and docs/en/architecture.md for diagrams.
 */
#include <chrono>
#include <cstring>
#include <iostream>
#include <thread>

#include "configuration_manager.h"
#include "daemon_manager.h"
#include "default_config.h"
#include "dll_functions.h"
#include "log_manager.h"
#include "server.h"
#include "utility.h"

// Definition of the global logger declared in log_manager.h (created below)
LogManager* g_log_manager = nullptr;

// Function to print usage instructions
static void print_usage(const char* prog) {
    std::cout << "Usage: " << prog << " [config_file_path] [dll_file_path]\n";
    std::cout << "Options:\n";
    std::cout << "  -h                    Show this help message\n";
    std::cout << "  config_file_path      Path to the configuration file (default: ./config.ini)\n";
    std::cout << "  dll_file_path         Path to the plugin library (default: dll_file in the config,\n"
                 "                        or ./libhandler.so)\n";
}

/**
 * @param argc / argv command line; also handed to every handle_init() call
 * @return 0 after a clean shutdown, 1 if anything failed during start-up.
 */
int main(int argc, char** argv) {
    // Default file paths
    std::string config_file = "./config.ini";
    std::string dll_file;

    // Parse command-line arguments
    if (argc > 1) {
        std::string arg1 = argv[1];
        if (arg1 == "-h" || arg1 == "--help") {
            print_usage(argv[0]);
            return 0;
        }
        config_file = argv[1]; // First argument is the configuration file path
    }
    if (argc > 2) {
        dll_file = argv[2]; // Second argument is the plugin path
    }

    // Load the configuration file
    // No logger yet (its settings are in this file), so errors go to stderr.
    // Relative paths in the config (bind_file, log_dir, dll_file) are relative
    // to the current directory, not to the config file.
    ConfigurationManager& config = ConfigurationManager::getInstance();
    if (!config.load_configuration(config_file)) {
        std::cerr << "[" + Utility::get_current_timestamp_string() + "] Failed to load configuration from "
                  << Utility::getCwd() << ", " << config_file << std::endl;
        return 1;
    }
    // The command line wins over the dll_file key
    if (dll_file.empty()) {
        dll_file = config.get_string("dll_file", DEFAULT_DLL_FILE);
    }

    install_signal_handlers();

    // Daemonize the server if configured to run in the background (before any thread is created)
    if (config.get_string("run_mode", DEFAULT_RUN_MODE) == "background") {
        if (start_daemon(argc, argv) < 0) {
            std::cerr << "[" + Utility::get_current_timestamp_string() + "] Failed to start as daemon" << std::endl;
            return 1;
        }
    }

    // Initialize logger
    int log_dest = config.get_integer("log_dest", DEFAULT_LOG_DEST);
    if (log_dest < 1 || log_dest > 3) {
        log_dest = DEFAULT_LOG_DEST;
    }
    g_log_manager = new LogManager(config.get_string("log_dir", DEFAULT_LOG_DIR),
                                   config.get_integer("log_level", DEFAULT_LOG_LEVEL),
                                   config.get_integer("log_maxfiles", DEFAULT_LOG_MAXFILES),
                                   config.get_int64("log_size", DEFAULT_LOG_SIZE),
                                   static_cast<LogDestination>(log_dest), config.get_string("log_prefix", ""));
    LogManager::set_thread_name("main");
    LOG_INFO("Configuration %s loaded, plugin %s", config_file.c_str(), dll_file.c_str());

    std::string pid_file = config.get_string("pid_file", DEFAULT_PID_FILE);
    if (!write_pid_file(pid_file)) {
        LOG_WARN("Failed to write pid file %s", pid_file.c_str());
    }

    // Load the plugin
    // dlopen + dlsym of every handle_* callback (see dll_functions.h)
    dll_func_t dll_functions;
    if (!load_dll_functions(&dll_functions, dll_file.c_str())) {
        LOG_ERR("Error loading plugin from %s", dll_file.c_str());
        return 1;
    }

    // First plugin callback: global initialisation in the main thread (read
    // config keys, allocate shared state). Non zero aborts the start.
    if (dll_functions.handle_init &&
        dll_functions.handle_init(argc, argv, static_cast<int>(ThreadType::MAIN)) != 0) {
        LOG_ERR("Main thread handle_init failed.");
        return 1;
    }

    int exit_code = 0;
    {
        // Start the server
        // Scoped, so the Server (and its threads) is destroyed before
        // handle_fini(MAIN) and the logger below.
        Server server(&dll_functions);
        server.save_argc_argv(argc, argv);
        if (server.start(config.get_string("bind_file", DEFAULT_BIND_FILE)) != 0) {
            LOG_ERR("Server start failed! Current dir: %s", Utility::getCwd().c_str());
            exit_code = 1;
        } else {
            // Main loop: just watch the flags set by the signal handlers. A
            // 100 ms sleep is a simple, portable alternative to sigwait() etc.
            while (!stop_signal) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                if (reopen_log_signal) {
                    reopen_log_signal = 0;
                    g_log_manager->reopen();
                    LOG_NOTICE("SIGHUP received, log file reopened");
                }
            }
            LOG_NOTICE("Stop signal received, shutting down...");
        }
        // Stop the server
        // Joins the network thread and the workers (each runs handle_fini)
        server.stop();
    }

    // Last plugin callback, in the main thread, after all other threads ended
    if (dll_functions.handle_fini) {
        dll_functions.handle_fini(static_cast<int>(ThreadType::MAIN));
    }
    remove_pid_file(pid_file);
    LOG_NOTICE("Server exit.");
    stop_daemon();

    // The plugin is intentionally not dlclose()d: thread_local destructors
    // registered by it may still run at process exit.
    delete g_log_manager;
    g_log_manager = nullptr;
    return exit_code;
}

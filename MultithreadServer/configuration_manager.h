/**
 * @file configuration_manager.h
 * @brief Process wide key/value configuration (the server's config.ini).
 *
 * File format (see TestHandler/conf/server_config.ini for an example):
 *
 *     # comment line           ; also a comment line
 *     [any section]            section headers are ignored (one flat namespace)
 *     worker_num = 4           key = value, whitespace around both is trimmed
 *     log_dir = ./log  # note  a '#' or ';' after whitespace starts a comment
 *
 * Later lines override earlier ones with the same key. The file is read
 * once at start-up (main.cpp) and never changed afterwards, so all threads
 * may read it concurrently without locking. Plugins read their own keys
 * from the same file with srv_conf_string() / srv_conf_int().
 *
 * The defaults for the server's own keys are in default_config.h.
 */
#ifndef CONFIGURATION_MANAGER_H
#define CONFIGURATION_MANAGER_H

#include <string>
#include <unordered_map>

// Key/value configuration loaded from an ini-like file:
//   key = value      # comment
// Lines starting with '#' or ';' and [section] headers are ignored.
class ConfigurationManager {
public:
    /** @return the single instance (a function local static: created on first use, thread safe in C++11). */
    static ConfigurationManager& getInstance();

    /**
     * Parse `file_path` and add its keys. Invalid lines (no '=') are
     * reported on stderr and skipped - the logger does not exist yet at
     * this point, because its settings come from this very file.
     * @return false if the file cannot be opened.
     */
    bool load_configuration(const std::string& file_path);

    /** @return the value of `key` (possibly empty), or `default_value` if the key is missing. */
    std::string get_string(const std::string& key, const std::string& default_value = "") const;

    /** Same as get_int64(), truncated to int. */
    int get_integer(const std::string& key, int default_value) const;
    /**
     * Parse the value with strtoll(base 0): decimal, "0x.." hex or a leading
     * 0 for octal (so "010" is 8!). Parsing stops at the first non digit
     * ("64k" reads as 64).
     * @return the number, or `default_value` if the key is missing, empty or
     *         does not start with a number.
     */
    long long get_int64(const std::string& key, long long default_value) const;

    /**
     * @return true for "1", "true", "yes" or "on" (lower case), false for any
     *         other non empty value, `default_value` if missing or empty.
     */
    bool get_bool(const std::string& key, bool default_value) const;

    // Returns a pointer that stays valid for the process lifetime (settings are
    // never modified after load_configuration)
    /** Backs srv_conf_string(): a C string the plugin may keep. */
    const char* get_cstring(const std::string& key, const char* default_value) const;

private:
    std::unordered_map<std::string, std::string> settings_; // key -> raw (trimmed) value
};

#endif // CONFIGURATION_MANAGER_H

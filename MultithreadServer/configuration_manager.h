#ifndef CONFIGURATION_MANAGER_H
#define CONFIGURATION_MANAGER_H

#include <string>
#include <unordered_map>

// Key/value configuration loaded from an ini-like file:
//   key = value      # comment
// Lines starting with '#' or ';' and [section] headers are ignored.
class ConfigurationManager {
public:
    static ConfigurationManager& getInstance();

    // Loads configuration from a file
    bool load_configuration(const std::string& file_path);

    // Retrieves a string configuration value
    std::string get_string(const std::string& key, const std::string& default_value = "") const;

    // Retrieves an integer configuration value (default_value if missing or invalid)
    int get_integer(const std::string& key, int default_value) const;
    long long get_int64(const std::string& key, long long default_value) const;

    bool get_bool(const std::string& key, bool default_value) const;

    // Returns a pointer that stays valid for the process lifetime (settings are
    // never modified after load_configuration)
    const char* get_cstring(const std::string& key, const char* default_value) const;

private:
    std::unordered_map<std::string, std::string> settings_;
};

#endif // CONFIGURATION_MANAGER_H

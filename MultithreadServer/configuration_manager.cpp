/**
 * @file configuration_manager.cpp
 * @brief Parsing of the ini-like configuration file and typed getters.
 *
 * Errors are written to std::cerr (with a timestamp) instead of the log:
 * the configuration is loaded before the LogManager exists.
 */
#include "configuration_manager.h"

#include <cctype>
#include <cstdlib>
#include <fstream>
#include <iostream>

#include "utility.h"

// Meyers singleton: constructed on the first call, destroyed at exit.
ConfigurationManager& ConfigurationManager::getInstance() {
    static ConfigurationManager cm;
    return cm;
}

/**
 * Line by line: strip the comment, trim, skip empty lines and [sections],
 * split at the first '=' (so values may contain '=').
 * @param file_path path of the configuration file
 * @return false if the file cannot be opened; invalid lines are only reported.
 */
bool ConfigurationManager::load_configuration(const std::string& file_path) {
    std::ifstream config_file(file_path);
    if (!config_file.is_open()) {
        std::cerr << "[" + Utility::get_current_timestamp_string() + "] Failed to open configuration file: "
                  << file_path << std::endl;
        return false;
    }

    std::string line;
    while (std::getline(config_file, line)) {
        // Strip comments: a '#' at the line start or preceded by whitespace
        // (same for ';'). Requiring whitespace in front keeps values such as
        // "a#b" intact.
        for (size_t i = 0; i < line.size(); ++i) {
            if ((line[i] == '#' || line[i] == ';') && (i == 0 || std::isspace(static_cast<unsigned char>(line[i - 1])))) {
                line.erase(i);
                break;
            }
        }
        line = Utility::trim(line);
        if (line.empty() || line[0] == '[') {
            continue;
        }

        size_t pos = line.find('=');
        if (pos == std::string::npos) {
            std::cerr << "[" + Utility::get_current_timestamp_string() + "] Ignore invalid config line: " << line
                      << std::endl;
            continue;
        }
        std::string key = Utility::trim(line.substr(0, pos));
        std::string value = Utility::trim(line.substr(pos + 1));
        if (!key.empty()) {
            settings_[key] = value; // a repeated key overrides the earlier value
        }
    }
    return true;
}

/** @return the raw value, or `default_value` if the key does not exist. */
std::string ConfigurationManager::get_string(const std::string& key, const std::string& default_value) const {
    auto it = settings_.find(key);
    if (it != settings_.end()) {
        return it->second;
    }
    return default_value;
}

/** int version of get_int64() (values outside the int range are truncated). */
int ConfigurationManager::get_integer(const std::string& key, int default_value) const {
    return static_cast<int>(get_int64(key, default_value));
}

/** See the header: strtoll with base 0, default on missing / empty / non numeric values. */
long long ConfigurationManager::get_int64(const std::string& key, long long default_value) const {
    auto it = settings_.find(key);
    if (it == settings_.end() || it->second.empty()) {
        return default_value;
    }
    char* end = nullptr;
    // base 0: "123" decimal, "0x7b" hex, "0173" octal
    long long value = std::strtoll(it->second.c_str(), &end, 0);
    // `end` did not move: not a single digit could be parsed
    if (end == it->second.c_str()) {
        std::cerr << "[" + Utility::get_current_timestamp_string() + "] Invalid integer for " << key << ": "
                  << it->second << ", use default " << default_value << std::endl;
        return default_value;
    }
    return value;
}

/** Accepts 1/true/yes/on as true (case sensitive); everything else non empty is false. */
bool ConfigurationManager::get_bool(const std::string& key, bool default_value) const {
    auto it = settings_.find(key);
    if (it == settings_.end() || it->second.empty()) {
        return default_value;
    }
    const std::string& v = it->second;
    return v == "1" || v == "true" || v == "yes" || v == "on";
}

/**
 * @return a pointer into the stored std::string. Safe to keep because the
 *         map is never modified after loading. Like get_string() (and unlike
 *         the numeric getters), an existing but empty value returns "" rather
 *         than `default_value`.
 */
const char* ConfigurationManager::get_cstring(const std::string& key, const char* default_value) const {
    auto it = settings_.find(key);
    return it == settings_.end() ? default_value : it->second.c_str();
}

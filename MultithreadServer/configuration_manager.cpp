#include "configuration_manager.h"

#include <cctype>
#include <cstdlib>
#include <fstream>
#include <iostream>

#include "utility.h"

ConfigurationManager& ConfigurationManager::getInstance() {
    static ConfigurationManager cm;
    return cm;
}

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
            settings_[key] = value;
        }
    }
    return true;
}

std::string ConfigurationManager::get_string(const std::string& key, const std::string& default_value) const {
    auto it = settings_.find(key);
    if (it != settings_.end()) {
        return it->second;
    }
    return default_value;
}

int ConfigurationManager::get_integer(const std::string& key, int default_value) const {
    return static_cast<int>(get_int64(key, default_value));
}

long long ConfigurationManager::get_int64(const std::string& key, long long default_value) const {
    auto it = settings_.find(key);
    if (it == settings_.end() || it->second.empty()) {
        return default_value;
    }
    char* end = nullptr;
    long long value = std::strtoll(it->second.c_str(), &end, 0);
    if (end == it->second.c_str()) {
        std::cerr << "[" + Utility::get_current_timestamp_string() + "] Invalid integer for " << key << ": "
                  << it->second << ", use default " << default_value << std::endl;
        return default_value;
    }
    return value;
}

bool ConfigurationManager::get_bool(const std::string& key, bool default_value) const {
    auto it = settings_.find(key);
    if (it == settings_.end() || it->second.empty()) {
        return default_value;
    }
    const std::string& v = it->second;
    return v == "1" || v == "true" || v == "yes" || v == "on";
}

const char* ConfigurationManager::get_cstring(const std::string& key, const char* default_value) const {
    auto it = settings_.find(key);
    return it == settings_.end() ? default_value : it->second.c_str();
}

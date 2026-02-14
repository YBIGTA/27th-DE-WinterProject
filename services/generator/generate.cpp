/**
 * Uber-Style Traffic Simulator (FIXED VERSION)
 * 
 * Fixes applied:
 * 1. Fixed busy-wait spin loop in schedule_events
 * 2. Added proper sleep/yield for short waits
 * 3. Added debug logging for timing verification
 * 4. Improved timestamp handling
 */

#include <iostream>
#include <vector>
#include <string>
#include <queue>
#include <thread>
#include <chrono>
#include <map>
#include <unordered_map>
#include <cmath>
#include <algorithm>
#include <sstream>
#include <iomanip>
#include <random>
#include <filesystem>
#include <memory>
#include <fstream>
#include <cctype>
#include <condition_variable>
#include <cstdlib>
#include <deque>
#include <functional>
#include <ctime>
#include <mutex>
#include <atomic>
#include <utility>

// Apache Arrow & Parquet Headers
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>

#if defined(__unix__) || defined(__APPLE__)
#include <netdb.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#include <csignal>
#endif

using namespace std;
using namespace std::chrono;
namespace fs = std::filesystem;

// ==========================================
// 1. Data Structures & Enums
// ==========================================

struct RawTripData {
    int64_t trip_id;
    
    int64_t VendorID;
    int64_t passenger_count;
    int64_t RatecodeID;
    int64_t payment_type;
    string store_and_fwd_flag;
    
    int64_t PULocationID;
    int64_t DOLocationID;
    
    double congestion_surcharge;
    double extra;
    double fare_amount;
    double improvement_surcharge;
    double mta_tax;
    double tip_amount;
    double tolls_amount;
    double total_amount;
    double trip_distance;
    double Airport_fee;
    
    int64_t tpep_pickup_datetime;  // Microseconds since epoch
    int64_t tpep_dropoff_datetime; // Microseconds since epoch
};

enum class EventType { PICKUP, IN_TRANSIT, DROPOFF };

struct SimulationEvent {
    EventType type;
    int64_t event_time_us; // Simulation time in microseconds

    const RawTripData* raw_data;

    bool operator>(const SimulationEvent& other) const {
        return event_time_us > other.event_time_us;
    }
};

struct PayloadWithRetry {
    string payload;
    size_t retry_count = 0;

    static constexpr size_t MAX_RETRIES = 3;

    bool can_retry() const {
        return retry_count < MAX_RETRIES;
    }

    void increment_retry() {
        retry_count++;
    }
};

// Batching: Accumulates events until size threshold or timeout
class BatchAccumulator {
private:
    std::vector<PayloadWithRetry> batch_;
    std::chrono::steady_clock::time_point first_event_time_;
    size_t max_batch_size_;
    std::chrono::microseconds timeout_us_;
    bool has_events_ = false;

public:
    BatchAccumulator(size_t max_size, std::chrono::microseconds timeout)
        : max_batch_size_(max_size), timeout_us_(timeout) {
        batch_.reserve(max_size);
    }

    // Returns true if batch is ready to send
    bool should_flush() const {
        if (!has_events_) return false;
        if (batch_.size() >= max_batch_size_) return true;

        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
            now - first_event_time_
        );
        return elapsed >= timeout_us_;
    }

    void add(PayloadWithRetry&& item) {
        if (!has_events_) {
            first_event_time_ = std::chrono::steady_clock::now();
            has_events_ = true;
        }
        batch_.push_back(std::move(item));
    }

    std::vector<PayloadWithRetry> take_batch() {
        auto result = std::move(batch_);
        batch_.clear();
        batch_.reserve(max_batch_size_);
        has_events_ = false;
        return result;
    }

    size_t size() const { return batch_.size(); }
    bool empty() const { return !has_events_ || batch_.empty(); }

    int64_t get_wait_time_us() const {
        if (!has_events_) return 0;
        auto now = std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::microseconds>(
            now - first_event_time_
        ).count();
    }
};

// Lock-free metrics for batching performance monitoring
class BatchMetrics {
private:
    std::atomic<int64_t> total_events_batched_{0};
    std::atomic<int64_t> total_batches_sent_{0};
    std::atomic<int64_t> total_wait_time_us_{0};
    std::atomic<int64_t> max_wait_time_us_{0};

    static constexpr size_t WINDOW_SIZE = 1000;
    std::array<std::atomic<int64_t>, WINDOW_SIZE> wait_times_;
    std::atomic<size_t> write_index_{0};

public:
    BatchMetrics() {
        for (auto& slot : wait_times_) {
            slot.store(0, std::memory_order_relaxed);
        }
    }

    void record_batch(int size, int64_t wait_us) {
        total_events_batched_.fetch_add(size, std::memory_order_relaxed);
        total_batches_sent_.fetch_add(1, std::memory_order_relaxed);
        total_wait_time_us_.fetch_add(wait_us, std::memory_order_relaxed);

        // Update max wait time using CAS loop
        int64_t current_max = max_wait_time_us_.load(std::memory_order_acquire);
        while (wait_us > current_max) {
            if (max_wait_time_us_.compare_exchange_weak(current_max, wait_us)) {
                break;
            }
        }

        size_t idx = write_index_.fetch_add(1, std::memory_order_relaxed) % WINDOW_SIZE;
        wait_times_[idx].store(wait_us, std::memory_order_relaxed);
    }

    int64_t get_avg_wait_time_us() const {
        int64_t total = total_wait_time_us_.load(std::memory_order_relaxed);
        int64_t count = total_batches_sent_.load(std::memory_order_relaxed);
        return count > 0 ? total / count : 0;
    }

    int64_t get_max_wait_time_us() const {
        return max_wait_time_us_.load(std::memory_order_relaxed);
    }

    double get_avg_batch_size() const {
        int64_t events = total_events_batched_.load(std::memory_order_relaxed);
        int64_t batches = total_batches_sent_.load(std::memory_order_relaxed);
        return batches > 0 ? static_cast<double>(events) / batches : 0.0;
    }

    int64_t get_total_batches() const {
        return total_batches_sent_.load(std::memory_order_relaxed);
    }
};

struct DateRange {
    int start_year;
    int start_month;
    int end_year;
    int end_month;
};

static constexpr const char* DEFAULT_DATA_PATH = "../../data/taxi_data_preprocessed";
static constexpr const char* DEFAULT_ENV_PATH = "../../config/.env";
static constexpr const char* DEFAULT_ZONE_CSV_PATH = "../../data/taxi_zones/taxi_zone_median_coords.csv";
static constexpr const char* DEFAULT_CONFIG_PATH = "config/default.yaml";

struct SimulationConfig {
    double playback_speed = 100.0;
    DateRange range{2020, 1, 2024, 4};
    string data_path = DEFAULT_DATA_PATH;
    string ingestion_url = "http://localhost:8080/ingest";
    bool debug_timing = false;  // NEW: Enable timing debug logs

    // Rate limiting config
    double rate_limit_threshold = 0.10;      // 10% 429s triggers backoff
    int64_t rate_limit_max_delay_ms = 5000;  // 5 seconds max delay

    // Circuit breaker config
    double circuit_breaker_threshold = 0.50;  // 50% failure rate
    size_t circuit_breaker_min_requests = 20;
    int circuit_breaker_timeout_sec = 30;

    // Connection management config
    size_t connection_max_failures = 3;

    // Retry & DLQ config
    size_t max_retries = 3;
    string dlq_filepath = "dead_letter_queue.jsonl";

    // Batching config
    size_t batch_size = 200;
    int64_t batch_timeout_ms = 100;
};

static string trim_copy(const string& value) {
    size_t start = value.find_first_not_of(" \t\r\n");
    if (start == string::npos) return "";
    size_t end = value.find_last_not_of(" \t\r\n");
    return value.substr(start, end - start + 1);
}

static string to_lower_copy(string value) {
    transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        return static_cast<char>(tolower(ch));
    });
    return value;
}

static bool parse_year_month_value(const string& value, int& year, int& month) {
    string trimmed = trim_copy(value);
    if (trimmed.size() >= 2 && trimmed.front() == '"' && trimmed.back() == '"') {
        trimmed = trimmed.substr(1, trimmed.size() - 2);
    }
    if (trimmed.size() != 7 || trimmed[4] != '-') return false;
    if (!isdigit(static_cast<unsigned char>(trimmed[0])) ||
        !isdigit(static_cast<unsigned char>(trimmed[1])) ||
        !isdigit(static_cast<unsigned char>(trimmed[2])) ||
        !isdigit(static_cast<unsigned char>(trimmed[3])) ||
        !isdigit(static_cast<unsigned char>(trimmed[5])) ||
        !isdigit(static_cast<unsigned char>(trimmed[6]))) {
        return false;
    }
    int y = stoi(trimmed.substr(0, 4));
    int m = stoi(trimmed.substr(5, 2));
    if (m < 1 || m > 12) return false;
    year = y;
    month = m;
    return true;
}

static void load_env_file(const string& env_file_path) {
    ifstream file(env_file_path);
    if (!file.is_open()) return; // .env file is optional

    string line;
    while (getline(file, line)) {
        string trimmed = trim_copy(line);
        if (trimmed.empty() || trimmed[0] == '#') continue;

        size_t eq = trimmed.find('=');
        if (eq == string::npos) continue;

        string key = trim_copy(trimmed.substr(0, eq));
        string value = trim_copy(trimmed.substr(eq + 1));

        // Remove quotes
        if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
            value = value.substr(1, value.size() - 2);
        }

        // Set env var (only if not already set)
        #ifdef _WIN32
            _putenv_s(key.c_str(), value.c_str());
        #else
            setenv(key.c_str(), value.c_str(), 0);
        #endif
    }
}

static string strip_quotes(const string& value) {
    if (value.size() >= 2) {
        if ((value.front() == '"' && value.back() == '"') ||
            (value.front() == '\'' && value.back() == '\'')) {
            return value.substr(1, value.size() - 2);
        }
    }
    return value;
}

static bool ends_with(const string& value, const string& suffix) {
    if (value.size() < suffix.size()) return false;
    return equal(suffix.rbegin(), suffix.rend(), value.rbegin());
}

static unordered_map<string, string> load_yaml_kv(const string& path) {
    unordered_map<string, string> kv;
    ifstream file(path);
    if (!file.is_open()) return kv;

    vector<pair<int, string>> sections;
    string line;
    while (getline(file, line)) {
        string trimmed = trim_copy(line);
        if (trimmed.empty() || trimmed[0] == '#') continue;

        int indent = 0;
        for (char ch : line) {
            if (ch == ' ') indent += 1;
            else if (ch == '\t') indent += 2;
            else break;
        }

        string content = trim_copy(line);
        size_t colon = content.find(':');
        if (colon == string::npos) continue;

        string key = trim_copy(content.substr(0, colon));
        string rest = trim_copy(content.substr(colon + 1));
        if (key.empty()) continue;

        while (!sections.empty() && indent <= sections.back().first) {
            sections.pop_back();
        }

        if (rest.empty()) {
            sections.push_back({indent, key});
            continue;
        }

        string full_key;
        for (size_t i = 0; i < sections.size(); i++) {
            if (i > 0) full_key += ".";
            full_key += sections[i].second;
        }
        if (!full_key.empty()) full_key += ".";
        full_key += key;

        string value = strip_quotes(rest);
        kv[to_lower_copy(full_key)] = value;
    }

    return kv;
}

static void apply_config_value(SimulationConfig& config, const string& raw_key, const string& raw_value) {
    string key = to_lower_copy(raw_key);
    string value = strip_quotes(raw_value);

    if (key == "playback_speed" || key == "playback.speed") {
        try { config.playback_speed = stod(value); } catch (...) {}
    } else if (key == "start" || key == "start_date" || key == "date_range.start") {
        int year = 0, month = 0;
        if (parse_year_month_value(value, year, month)) {
            config.range.start_year = year;
            config.range.start_month = month;
        }
    } else if (key == "end" || key == "end_date" || key == "date_range.end") {
        int year = 0, month = 0;
        if (parse_year_month_value(value, year, month)) {
            config.range.end_year = year;
            config.range.end_month = month;
        }
    } else if (key == "data_path" || key == "data.path") {
        if (!value.empty()) config.data_path = value;
    } else if (key == "ingestion_url" || key == "ingest_url") {
        if (!value.empty()) config.ingestion_url = value;
    } else if (key == "debug_timing") {
        config.debug_timing = (value == "true" || value == "1");
    } else if (key == "rate_limit_threshold" || key == "rate_limit.threshold") {
        try { config.rate_limit_threshold = stod(value); } catch (...) {}
    } else if (key == "rate_limit_max_delay_ms" || key == "rate_limit.max_delay_ms") {
        try { config.rate_limit_max_delay_ms = stoll(value); } catch (...) {}
    } else if (key == "circuit_breaker_threshold" || key == "circuit_breaker.threshold") {
        try { config.circuit_breaker_threshold = stod(value); } catch (...) {}
    } else if (key == "circuit_breaker_min_requests" || key == "circuit_breaker.min_requests") {
        try { config.circuit_breaker_min_requests = stoull(value); } catch (...) {}
    } else if (key == "circuit_breaker_timeout_sec" || key == "circuit_breaker.timeout_sec") {
        try { config.circuit_breaker_timeout_sec = stoi(value); } catch (...) {}
    } else if (key == "connection_max_failures" || key == "connection.max_failures") {
        try { config.connection_max_failures = stoull(value); } catch (...) {}
    } else if (key == "max_retries" || key == "retry.max_retries") {
        try { config.max_retries = stoull(value); } catch (...) {}
    } else if (key == "dlq_filepath" || key == "dlq.filepath") {
        if (!value.empty()) config.dlq_filepath = value;
    } else if (key == "batch_size" || key == "batch.size") {
        try { config.batch_size = stoull(value); } catch (...) {}
    } else if (key == "batch_timeout_ms" || key == "batch.timeout_ms") {
        try { config.batch_timeout_ms = stoll(value); } catch (...) {}
    }
}

static string resolve_ingestion_url_from_env() {
    const char* env_url = getenv("INGEST_URL");
    if (env_url && *env_url) return string(env_url);

    const char* nginx_ip = getenv("NGINX_IP");
    const char* lb_port = getenv("NGINX_LB_PORT");
    if (nginx_ip && *nginx_ip && lb_port && *lb_port) {
        return string("http://") + nginx_ip + ":" + lb_port + "/ingest";
    }

    return "";
}

static SimulationConfig load_config(const string& path) {
    // Load .env file first
    load_env_file(DEFAULT_ENV_PATH);

    SimulationConfig config;
    ifstream file(path);
    if (!file.is_open()) {
        cerr << "[Config] Using defaults (file not found: " << path << ")" << endl;
        string env_url = resolve_ingestion_url_from_env();
        if (!env_url.empty()) config.ingestion_url = env_url;
        return config;
    }

    if (ends_with(path, ".yaml") || ends_with(path, ".yml")) {
        auto kv = load_yaml_kv(path);
        for (const auto& entry : kv) {
            apply_config_value(config, entry.first, entry.second);
        }
    } else {
        string line;
        while (getline(file, line)) {
            string trimmed = trim_copy(line);
            if (trimmed.empty() || trimmed[0] == '#') continue;
            size_t eq = trimmed.find('=');
            if (eq == string::npos) continue;

            string key = trim_copy(trimmed.substr(0, eq));
            string value = trim_copy(trimmed.substr(eq + 1));
            apply_config_value(config, key, value);
        }
    }

    string env_url = resolve_ingestion_url_from_env();
    if (!env_url.empty()) config.ingestion_url = env_url;

    return config;
}

template<typename T>
class BoundedQueue {
public:
    explicit BoundedQueue(size_t capacity) : capacity_(capacity) {}

    bool push(T item) {
        unique_lock<mutex> lock(mu_);
        not_full_.wait(lock, [&] { return closed_ || queue_.size() < capacity_; });
        if (closed_) return false;
        queue_.push_back(std::move(item));
        not_empty_.notify_one();
        return true;
    }

    bool pop(T& out) {
        unique_lock<mutex> lock(mu_);
        not_empty_.wait(lock, [&] { return closed_ || !queue_.empty(); });
        if (queue_.empty()) return false;
        out = std::move(queue_.front());
        queue_.pop_front();
        not_full_.notify_one();
        return true;
    }

    // Returns: 1 = got item, 0 = timeout, -1 = closed
    int try_pop_for(T& out, std::chrono::milliseconds timeout) {
        unique_lock<mutex> lock(mu_);
        bool got = not_empty_.wait_for(lock, timeout, [&] { return closed_ || !queue_.empty(); });
        if (!got) return 0;  // timeout
        if (queue_.empty()) return -1;  // closed
        out = std::move(queue_.front());
        queue_.pop_front();
        not_full_.notify_one();
        return 1;
    }

    void close() {
        lock_guard<mutex> lock(mu_);
        closed_ = true;
        not_empty_.notify_all();
        not_full_.notify_all();
    }

private:
    size_t capacity_;
    deque<T> queue_;
    mutex mu_;
    condition_variable not_full_;
    condition_variable not_empty_;
    bool closed_ = false;
};

struct HttpEndpoint {
    string host;
    string port;
    string path;
};

static bool parse_http_url(const string& url, HttpEndpoint& out) {
    const string prefix = "http://";
    if (url.rfind(prefix, 0) != 0) return false;
    string rest = url.substr(prefix.size());
    size_t slash = rest.find('/');
    string host_port = (slash == string::npos) ? rest : rest.substr(0, slash);
    out.path = (slash == string::npos) ? "/" : rest.substr(slash);
    size_t colon = host_port.find(':');
    if (colon == string::npos) {
        out.host = host_port;
        out.port = "80";
    } else {
        out.host = host_port.substr(0, colon);
        out.port = host_port.substr(colon + 1);
        if (out.port.empty()) out.port = "80";
    }
    return !out.host.empty();
}

#if defined(__unix__) || defined(__APPLE__)
static bool send_all(int sock, const char* data, size_t len) {
    const char* cursor = data;
    size_t remaining = len;
    while (remaining > 0) {
        ssize_t sent = send(sock, cursor, remaining, 0);
        if (sent <= 0) return false;
        cursor += sent;
        remaining -= static_cast<size_t>(sent);
    }
    return true;
}

static bool set_socket_timeouts(int sock) {
    timeval timeout{};
    timeout.tv_sec = 2;
    timeout.tv_usec = 0;
    if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) != 0) {
        return false;
    }
    if (setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) != 0) {
        return false;
    }
    return true;
}

static bool recv_more(int sock, string& buffer) {
    char temp[1024];
    ssize_t received = recv(sock, temp, sizeof(temp), 0);
    if (received <= 0) return false;
    buffer.append(temp, static_cast<size_t>(received));
    return true;
}

static bool read_chunked_body(int sock, string& pending) {
    while (true) {
        size_t line_end = pending.find("\r\n");
        while (line_end == string::npos) {
            if (!recv_more(sock, pending)) return false;
            line_end = pending.find("\r\n");
        }

        string line = pending.substr(0, line_end);
        pending.erase(0, line_end + 2);
        size_t chunk_size = strtoul(line.c_str(), nullptr, 16);
        if (chunk_size == 0) {
            // Consume trailer headers until an empty line.
            while (true) {
                line_end = pending.find("\r\n");
                while (line_end == string::npos) {
                    if (!recv_more(sock, pending)) return false;
                    line_end = pending.find("\r\n");
                }
                string trailer = pending.substr(0, line_end);
                pending.erase(0, line_end + 2);
                if (trailer.empty()) return true;
            }
        }

        while (pending.size() < chunk_size + 2) {
            if (!recv_more(sock, pending)) return false;
        }
        pending.erase(0, chunk_size + 2);
    }
}

static bool read_http_response(int sock, int& out_status_code) {
    string buffer;
    buffer.reserve(2048);
    size_t header_end = string::npos;
    while (header_end == string::npos) {
        if (!recv_more(sock, buffer)) {
            out_status_code = 0;  // Socket error
            return false;
        }
        header_end = buffer.find("\r\n\r\n");
        if (buffer.size() > 65536) {
            out_status_code = 0;
            return false;
        }
    }

    string header = buffer.substr(0, header_end);
    size_t body_offset = header_end + 4;
    string pending = buffer.substr(body_offset);

    // Parse HTTP status line: "HTTP/1.1 200 OK"
    istringstream header_stream(header);
    string status_line;
    getline(header_stream, status_line);
    if (!status_line.empty() && status_line.back() == '\r') {
        status_line.pop_back();
    }

    // Extract status code (between first and second space)
    size_t first_space = status_line.find(' ');
    if (first_space != string::npos && first_space + 1 < status_line.size()) {
        size_t second_space = status_line.find(' ', first_space + 1);
        string code_str = (second_space != string::npos)
            ? status_line.substr(first_space + 1, second_space - first_space - 1)
            : status_line.substr(first_space + 1);
        try {
            out_status_code = stoi(code_str);
        } catch (...) {
            out_status_code = 0;
        }
    } else {
        out_status_code = 0;
    }

    // Parse headers
    int64_t content_length = -1;
    bool chunked = false;
    string line;
    while (getline(header_stream, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        string lowered = to_lower_copy(line);
        if (lowered.rfind("content-length:", 0) == 0) {
            string value = trim_copy(line.substr(15));
            try { content_length = stoll(value); } catch (...) {}
        } else if (lowered.rfind("transfer-encoding:", 0) == 0) {
            if (lowered.find("chunked") != string::npos) chunked = true;
        }
    }

    if (chunked) {
        return read_chunked_body(sock, pending);
    }

    if (content_length >= 0) {
        int64_t remaining = content_length - static_cast<int64_t>(pending.size());
        while (remaining > 0) {
            char temp[1024];
            ssize_t received = recv(sock, temp, sizeof(temp), 0);
            if (received <= 0) return false;
            remaining -= received;
        }
        return true;
    }

    // No length info; drain until the server closes.
    while (true) {
        char temp[1024];
        ssize_t received = recv(sock, temp, sizeof(temp), 0);
        if (received == 0) return true;
        if (received < 0) return false;
    }
}

// ==========================================
// Connection Health Tracking (Forward Declaration)
// ==========================================

class ConnectionHealth {
private:
    size_t consecutive_failures_ = 0;
    size_t total_resets_ = 0;
    size_t max_failures_;

public:
    explicit ConnectionHealth(size_t max_failures) : max_failures_(max_failures) {}

    void record_success() {
        consecutive_failures_ = 0;
    }

    // Returns true if connection should be reset
    bool record_failure(bool is_socket_error) {
        consecutive_failures_++;

        // Socket errors -> immediate reset
        if (is_socket_error) return true;

        // HTTP errors -> reset after threshold
        return consecutive_failures_ >= max_failures_;
    }

    void record_reset() {
        consecutive_failures_ = 0;
        total_resets_++;
    }

    bool should_reset() const {
        return consecutive_failures_ >= max_failures_;
    }

    size_t get_total_resets() const {
        return total_resets_;
    }
};

class HttpConnection {
public:
    HttpConnection() = default;
    explicit HttpConnection(HttpEndpoint endpoint) : endpoint_(std::move(endpoint)) {}
    ~HttpConnection() { close_socket(); }

    bool matches(const HttpEndpoint& endpoint) const {
        return endpoint_.host == endpoint.host && endpoint_.port == endpoint.port &&
               endpoint_.path == endpoint.path;
    }

    void reset(HttpEndpoint endpoint) {
        close_socket();
        endpoint_ = std::move(endpoint);
    }

    bool post_json(const string& payload, int& out_status_code, ConnectionHealth& health) {
        if (!ensure_connected()) {
            out_status_code = 0;
            return false;
        }

        string request = "POST " + endpoint_.path + " HTTP/1.1\r\n";
        request += "Host: " + endpoint_.host + "\r\n";
        request += "Content-Type: application/json\r\n";
        request += "Content-Length: " + to_string(payload.size()) + "\r\n";
        request += "Connection: keep-alive\r\n\r\n";
        request += payload;

        if (!send_all(sock_, request.data(), request.size())) {
            out_status_code = 0;
            close_socket();
            health.record_failure(true);  // Socket error
            return false;
        }

        if (!read_http_response(sock_, out_status_code)) {
            close_socket();
            health.record_failure(true);  // Socket error during read
            return false;
        }

        // Valid HTTP response received
        if (out_status_code >= 200 && out_status_code < 300) {
            health.record_success();
            return true;
        } else {
            // HTTP error - keep connection unless too many failures
            if (health.record_failure(false)) {
                close_socket();
            }
            return false;
        }
    }

private:
    HttpEndpoint endpoint_;
    int sock_ = -1;

    void close_socket() {
        if (sock_ != -1) {
            close(sock_);
            sock_ = -1;
        }
    }

    bool ensure_connected() {
        if (sock_ != -1) return true;

        addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        addrinfo* res = nullptr;
        if (getaddrinfo(endpoint_.host.c_str(), endpoint_.port.c_str(), &hints, &res) != 0) {
            return false;
        }

        int sock = -1;
        for (addrinfo* rp = res; rp != nullptr; rp = rp->ai_next) {
            sock = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
            if (sock == -1) continue;
            if (connect(sock, rp->ai_addr, rp->ai_addrlen) == 0) break;
            close(sock);
            sock = -1;
        }
        freeaddrinfo(res);
        if (sock == -1) return false;
        if (!set_socket_timeouts(sock)) {
            close(sock);
            return false;
        }
        sock_ = sock;
        return true;
    }
};

// ==========================================
// Rate Limiter (Adaptive 429 Throttling)
// ==========================================

class RateLimiter {
private:
    // Circular buffer for 429 tracking
    static constexpr size_t WINDOW_SIZE = 100;
    std::array<std::atomic<bool>, WINDOW_SIZE> is_429_;
    std::atomic<size_t> write_index_{0};

    // Current delay (microseconds)
    std::atomic<int64_t> current_delay_us_{0};

    // Configuration
    double rate_threshold_;
    int64_t max_delay_us_;

    // Metrics
    std::atomic<size_t> total_429s_{0};
    std::atomic<size_t> total_requests_{0};

    mutable std::mutex log_mu_;

public:
    RateLimiter(double threshold, int64_t max_delay_ms)
        : rate_threshold_(threshold),
          max_delay_us_(max_delay_ms * 1000) {
        for (auto& slot : is_429_) {
            slot.store(false, std::memory_order_relaxed);
        }
    }

    void record_response(int status_code) {
        total_requests_.fetch_add(1, std::memory_order_relaxed);

        bool is_429 = (status_code == 429);
        size_t idx = write_index_.fetch_add(1, std::memory_order_relaxed) % WINDOW_SIZE;
        is_429_[idx].store(is_429, std::memory_order_relaxed);

        if (is_429) {
            total_429s_.fetch_add(1, std::memory_order_relaxed);
            adjust_delay_up();
        } else if (status_code >= 200 && status_code < 300) {
            adjust_delay_down();
        }
    }

    int64_t get_delay_us() const {
        return current_delay_us_.load(std::memory_order_acquire);
    }

    double get_429_rate() const {
        size_t count = 0;
        for (const auto& slot : is_429_) {
            if (slot.load(std::memory_order_relaxed)) count++;
        }
        return static_cast<double>(count) / WINDOW_SIZE;
    }

    size_t get_total_429s() const {
        return total_429s_.load(std::memory_order_relaxed);
    }

    size_t get_total_requests() const {
        return total_requests_.load(std::memory_order_relaxed);
    }

private:
    void adjust_delay_up() {
        int64_t current = current_delay_us_.load(std::memory_order_acquire);
        int64_t new_delay = (current == 0) ? 10000 : std::min(
            static_cast<int64_t>(current * 1.5), max_delay_us_
        );

        if (current_delay_us_.compare_exchange_strong(current, new_delay)) {
            std::lock_guard<std::mutex> lock(log_mu_);
            std::cerr << "[RATE_LIMIT] Increased delay: " << current/1000 << "ms -> "
                      << new_delay/1000 << "ms (429 detected)" << std::endl;
        }
    }

    void adjust_delay_down() {
        if (get_429_rate() >= rate_threshold_) return;

        int64_t current = current_delay_us_.load(std::memory_order_acquire);
        if (current == 0) return;

        int64_t new_delay = std::max(static_cast<int64_t>(current * 0.9), int64_t{0});
        if (current_delay_us_.compare_exchange_strong(current, new_delay)) {
            std::lock_guard<std::mutex> lock(log_mu_);
            std::cerr << "[RATE_LIMIT] Decreased delay: " << current/1000 << "ms -> "
                      << new_delay/1000 << "ms (recovering)" << std::endl;
        }
    }
};

// ==========================================
// Circuit Breaker Pattern
// ==========================================

class CircuitBreaker {
public:
    enum class State { CLOSED, OPEN, HALF_OPEN };

private:
    struct RequestRecord {
        std::chrono::steady_clock::time_point timestamp;
        bool is_failure;
    };

    std::atomic<State> state_{State::CLOSED};

    // Sliding window (protected by window_mu_)
    static constexpr size_t WINDOW_SIZE = 100;
    std::array<RequestRecord, WINDOW_SIZE> window_;
    size_t write_index_{0};
    mutable std::mutex window_mu_;

    // Configuration
    double failure_threshold_;
    size_t min_requests_;
    std::chrono::seconds open_timeout_;

    // OPEN state tracking (protected by state_mu_)
    std::chrono::steady_clock::time_point open_since_;

    // HALF_OPEN testing
    std::atomic<size_t> half_open_attempts_{0};
    std::atomic<size_t> half_open_successes_{0};
    static constexpr size_t TEST_COUNT = 5;
    static constexpr size_t SUCCESS_THRESHOLD = 3;

    // Metrics
    std::atomic<size_t> total_trips_{0};
    std::atomic<size_t> total_rejects_{0};

    mutable std::mutex state_mu_;

public:
    CircuitBreaker(double threshold, size_t min_req, int timeout_sec)
        : failure_threshold_(threshold),
          min_requests_(min_req),
          open_timeout_(timeout_sec) {
        // Initialize window with old timestamps
        auto old_time = std::chrono::steady_clock::now() - std::chrono::hours(1);
        for (auto& record : window_) {
            record.timestamp = old_time;
            record.is_failure = false;
        }
    }

    bool allow_request() {
        State current = state_.load(std::memory_order_acquire);

        if (current == State::CLOSED) return true;

        if (current == State::OPEN) {
            auto now = std::chrono::steady_clock::now();
            std::chrono::steady_clock::time_point open_time;
            {
                std::lock_guard<std::mutex> lock(state_mu_);
                open_time = open_since_;
            }
            if (now - open_time >= open_timeout_) {
                transition_to_half_open();
                return true;
            }
            total_rejects_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }

        if (current == State::HALF_OPEN) {
            size_t attempts = half_open_attempts_.fetch_add(1, std::memory_order_acquire);
            if (attempts < TEST_COUNT) return true;
            total_rejects_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }

        return false;
    }

    void record_result(int status_code) {
        bool is_server_error = (status_code == 0 || status_code >= 500);
        bool is_success = (status_code >= 200 && status_code < 300);

        State current = state_.load(std::memory_order_acquire);

        if (current == State::HALF_OPEN) {
            handle_half_open_result(is_success);
            return;
        }

        // Record in sliding window
        auto now = std::chrono::steady_clock::now();
        {
            std::lock_guard<std::mutex> lock(window_mu_);
            size_t idx = write_index_ % WINDOW_SIZE;
            window_[idx] = {now, is_server_error};
            write_index_++;
        }

        if (current == State::CLOSED && should_trip()) {
            transition_to_open();
        }
    }

    State get_state() const {
        return state_.load(std::memory_order_acquire);
    }

    size_t get_total_trips() const {
        return total_trips_.load(std::memory_order_relaxed);
    }

    size_t get_total_rejects() const {
        return total_rejects_.load(std::memory_order_relaxed);
    }

private:
    bool should_trip() const {
        auto now = std::chrono::steady_clock::now();
        auto time_window = std::chrono::seconds(10);
        size_t failure_count = 0;
        size_t total = 0;

        {
            std::lock_guard<std::mutex> lock(window_mu_);
            for (const auto& record : window_) {
                if (now - record.timestamp <= time_window) {
                    total++;
                    if (record.is_failure) failure_count++;
                }
            }
        }

        if (total < min_requests_) return false;

        double rate = static_cast<double>(failure_count) / total;
        return rate >= failure_threshold_;
    }

    void transition_to_open() {
        std::lock_guard<std::mutex> lock(state_mu_);
        State expected = State::CLOSED;
        if (state_.compare_exchange_strong(expected, State::OPEN)) {
            open_since_ = std::chrono::steady_clock::now();
            total_trips_.fetch_add(1, std::memory_order_relaxed);
            std::cerr << "[CIRCUIT_BREAKER] CLOSED -> OPEN (server unhealthy)" << std::endl;
        }
    }

    void transition_to_half_open() {
        std::lock_guard<std::mutex> lock(state_mu_);
        State expected = State::OPEN;
        if (state_.compare_exchange_strong(expected, State::HALF_OPEN)) {
            half_open_attempts_.store(0, std::memory_order_release);
            half_open_successes_.store(0, std::memory_order_release);
            std::cerr << "[CIRCUIT_BREAKER] OPEN -> HALF_OPEN (testing recovery)" << std::endl;
        }
    }

    void handle_half_open_result(bool success) {
        if (success) {
            size_t successes = half_open_successes_.fetch_add(1, std::memory_order_acquire) + 1;
            if (successes >= SUCCESS_THRESHOLD) {
                std::lock_guard<std::mutex> lock(state_mu_);
                State expected = State::HALF_OPEN;
                if (state_.compare_exchange_strong(expected, State::CLOSED)) {
                    std::cerr << "[CIRCUIT_BREAKER] HALF_OPEN -> CLOSED (recovered)" << std::endl;
                }
            }
        } else {
            std::lock_guard<std::mutex> lock(state_mu_);
            State expected = State::HALF_OPEN;
            if (state_.compare_exchange_strong(expected, State::OPEN)) {
                open_since_ = std::chrono::steady_clock::now();
                std::cerr << "[CIRCUIT_BREAKER] HALF_OPEN -> OPEN (test failed)" << std::endl;
            }
        }
    }
};

static bool http_post_json(const HttpEndpoint& endpoint, const string& payload,
                          int& out_status_code, size_t max_failures) {
    thread_local HttpConnection conn;
    thread_local ConnectionHealth health(max_failures);

    if (!conn.matches(endpoint)) {
        conn.reset(endpoint);
        health.record_reset();
    }

    if (conn.post_json(payload, out_status_code, health)) {
        return true;
    }

    // First attempt failed - retry with reset if needed
    if (health.should_reset()) {
        conn.reset(endpoint);
        health.record_reset();
    }

    return conn.post_json(payload, out_status_code, health);
}
#else
static bool http_post_json(const HttpEndpoint&, const string&, int& out_status_code, size_t) {
    out_status_code = 0;
    return false;
}
#endif

// Batch version of http_post_json: sends array of events to /ingest/batch
static bool http_post_json_batch(const HttpEndpoint& endpoint,
                                  const std::vector<PayloadWithRetry>& batch,
                                  int& out_status_code,
                                  size_t max_failures) {
    if (batch.empty()) {
        out_status_code = 200;
        return true;
    }

    // Build JSON array: "[{event1},{event2},...,{eventN}]"
    std::ostringstream json_array;
    json_array << "[";
    for (size_t i = 0; i < batch.size(); ++i) {
        if (i > 0) json_array << ",";
        json_array << batch[i].payload;
    }
    json_array << "]";

    std::string payload = json_array.str();

    // Modify endpoint to use /ingest/batch
    HttpEndpoint batch_endpoint = endpoint;
    batch_endpoint.path = "/ingest/batch";

    return http_post_json(batch_endpoint, payload, out_status_code, max_failures);
}


// ==========================================
// 2. Helper Services (GPS)
// ==========================================

class GeoService {
private:
    map<int, pair<double, double>> location_map;

    static string trim(const string& value) {
        size_t start = value.find_first_not_of(" \t\r\n");
        if (start == string::npos) return "";
        size_t end = value.find_last_not_of(" \t\r\n");
        return value.substr(start, end - start + 1);
    }

    static string strip_quotes(const string& value) {
        if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
            return value.substr(1, value.size() - 2);
        }
        return value;
    }

    bool load_from_csv(const string& path) {
        ifstream file(path);
        if (!file.is_open()) return false;

        string line;
        bool first = true;
        while (getline(file, line)) {
            if (line.empty()) continue;
            if (first) {
                first = false;
                if (line.find("LocationID") != string::npos) continue;
            }

            stringstream ss(line);
            string token;
            vector<string> fields;
            while (getline(ss, token, ',')) {
                fields.push_back(strip_quotes(trim(token)));
            }
            if (fields.size() < 3) continue;

            try {
                int location_id = stoi(fields[0]);
                double lon = stod(fields[1]);
                double lat = stod(fields[2]);
                location_map[location_id] = {lat, lon};
            } catch (...) {
                continue;
            }
        }

        return !location_map.empty();
    }

public:
    explicit GeoService(const string& csv_path = DEFAULT_ZONE_CSV_PATH) {
        if (!load_from_csv(csv_path)) {
            location_map[142] = {40.7711, -73.9841};
            location_map[230] = {40.7629, -73.9860};
            location_map[100] = {40.7505, -73.9934};
            location_map[161] = {40.7559, -73.9864};
        }
    }

    pair<double, double> get_coords(int location_id) {
        auto it = location_map.find(location_id);
        if (it != location_map.end()) return it->second;
        return {40.7580, -73.9855};  // Default NYC coords
    }

    /**
     * Linear interpolation between pickup and dropoff locations.
     * 
     * @param pu_id        Pickup location ID
     * @param do_id        Dropoff location ID  
     * @param start_ts     Pickup time (microseconds)
     * @param end_ts       Dropoff time (microseconds)
     * @param current_ts   Current simulation time (microseconds)
     * @return {latitude, longitude} interpolated position
     */
    pair<double, double> interpolate(int pu_id, int do_id, 
                                     int64_t start_ts, int64_t end_ts, int64_t current_ts) {
        auto start_pos = get_coords(pu_id);
        auto end_pos = get_coords(do_id);

        double total_duration = static_cast<double>(end_ts - start_ts);
        if (total_duration <= 0) return start_pos;

        double elapsed = static_cast<double>(current_ts - start_ts);
        double ratio = elapsed / total_duration;

        // Clamp ratio to [0, 1]
        if (ratio <= 0.0) return start_pos;
        if (ratio >= 1.0) return end_pos;

        double lat = start_pos.first + (end_pos.first - start_pos.first) * ratio;
        double lon = start_pos.second + (end_pos.second - start_pos.second) * ratio;
        return {lat, lon};
    }
};

// ==========================================
// Dead-Letter Queue (DLQ)
// ==========================================

class DeadLetterQueue {
private:
    std::string filepath_;
    std::ofstream file_;
    std::mutex file_mu_;
    std::atomic<size_t> total_written_{0};

public:
    explicit DeadLetterQueue(const std::string& filepath) : filepath_(filepath) {
        file_.open(filepath, std::ios::app);
        if (!file_.is_open()) {
            std::cerr << "[DLQ] Failed to open dead-letter file: " << filepath << std::endl;
        }
    }

    ~DeadLetterQueue() {
        if (file_.is_open()) {
            file_.close();
        }
    }

    bool write(const std::string& payload, size_t retry_count) {
        if (!file_.is_open()) return false;

        std::lock_guard<std::mutex> lock(file_mu_);

        // Format: timestamp|retry_count|payload
        auto now = std::chrono::system_clock::now();
        auto now_time_t = std::chrono::system_clock::to_time_t(now);

        file_ << now_time_t << "|" << retry_count << "|" << payload << "\n";
        file_.flush();

        total_written_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    size_t get_total_written() const {
        return total_written_.load(std::memory_order_relaxed);
    }
};

// ==========================================
// 3. Core Engine (Simulator) - FIXED
// ==========================================

class UberSimulator {
private:
    priority_queue<SimulationEvent, vector<SimulationEvent>, greater<SimulationEvent>> event_queue;
    deque<RawTripData> dataset;
    GeoService geo_service;
    double playback_speed;
    string ingestion_url;
    HttpEndpoint ingestion_endpoint;
    bool ingestion_endpoint_ok = false;
    bool debug_timing = false;
    mutex log_mu;
    mutex ingest_mu;
    
    const int64_t TRANSIT_UPDATE_INTERVAL_US = 30 * 1000000; // 30 seconds
    const size_t PAYLOAD_QUEUE_CAPACITY = 4096;

    // Statistics
    atomic<size_t> events_sent{0};
    atomic<size_t> pickup_count{0};
    atomic<size_t> transit_count{0};
    atomic<size_t> dropoff_count{0};

    // Resilience configuration
    double rate_limit_threshold;
    int64_t rate_limit_max_delay_ms;
    double circuit_breaker_threshold;
    size_t circuit_breaker_min_requests;
    int circuit_breaker_timeout_sec;
    size_t connection_max_failures;
    size_t max_retries;
    string dlq_filepath;

    // Batching configuration and metrics
    size_t batch_size;
    int64_t batch_timeout_ms;
    BatchMetrics batch_metrics;

public:
    UberSimulator(const SimulationConfig& config)
        : playback_speed(config.playback_speed),
          ingestion_url(config.ingestion_url),
          debug_timing(config.debug_timing),
          rate_limit_threshold(config.rate_limit_threshold),
          rate_limit_max_delay_ms(config.rate_limit_max_delay_ms),
          circuit_breaker_threshold(config.circuit_breaker_threshold),
          circuit_breaker_min_requests(config.circuit_breaker_min_requests),
          circuit_breaker_timeout_sec(config.circuit_breaker_timeout_sec),
          connection_max_failures(config.connection_max_failures),
          max_retries(config.max_retries),
          dlq_filepath(config.dlq_filepath),
          batch_size(config.batch_size),
          batch_timeout_ms(config.batch_timeout_ms) {
        ingestion_endpoint_ok = parse_http_url(ingestion_url, ingestion_endpoint);
        if (!ingestion_endpoint_ok) {
            lock_guard<mutex> lock(log_mu);
            cerr << "[Config] Invalid ingestion_url; falling back to stdout." << endl;
        }
    }

    void ingest_trip(RawTripData data) {
        vector<RawTripData> batch;
        batch.push_back(std::move(data));
        ingest_trips(std::move(batch));
    }

    void ingest_trips(vector<RawTripData>&& batch) {
        if (batch.empty()) return;
        lock_guard<mutex> lock(ingest_mu);
        for (auto& data : batch) {
            dataset.push_back(std::move(data));
            const RawTripData* p_data = &dataset.back();
            const auto& stored = *p_data;

            // 1. PICKUP event
            event_queue.push({EventType::PICKUP, stored.tpep_pickup_datetime, p_data});

            // 2. DROPOFF event
            event_queue.push({EventType::DROPOFF, stored.tpep_dropoff_datetime, p_data});
        }
    }

    void run() {
        if (event_queue.empty()) {
            cout << "No events to simulate." << endl;
            return;
        }

        cout << ">>> Starting Simulation (Speed: " << playback_speed << "x) <<<" << endl;
        cout << ">>> Total events queued: " << event_queue.size() << endl;

        int64_t sim_start_ts = event_queue.top().event_time_us;
        cout << ">>> First Event Time (Unix us): " << sim_start_ts << endl;
        cout << ">>> First Event Time (ISO): " << format_timestamp(sim_start_ts) << endl;

        cout << "[DEBUG] Creating queues..." << endl;
        // Initialize queues
        BoundedQueue<PayloadWithRetry> payload_queue(PAYLOAD_QUEUE_CAPACITY);
        BoundedQueue<PayloadWithRetry> requeue(1024);
        cout << "[DEBUG] Queues created" << endl;

        cout << "[DEBUG] Creating resilience objects..." << endl;
        // Initialize resilience objects
        RateLimiter rate_limiter(rate_limit_threshold, rate_limit_max_delay_ms);
        CircuitBreaker circuit_breaker(circuit_breaker_threshold,
                                       circuit_breaker_min_requests,
                                       circuit_breaker_timeout_sec);
        DeadLetterQueue dlq(dlq_filepath);
        cout << "[DEBUG] Resilience objects created" << endl;

        cout << "[DEBUG] Starting metrics thread..." << endl;
        // Metrics reporting thread
        atomic<bool> metrics_running{true};
        thread metrics_thread([&]() {
            while (metrics_running.load()) {
                this_thread::sleep_for(chrono::seconds(10));

                lock_guard<mutex> lock(log_mu);
                auto cb_state = circuit_breaker.get_state();
                string cb_state_str = (cb_state == CircuitBreaker::State::CLOSED) ? "CLOSED" :
                                     (cb_state == CircuitBreaker::State::OPEN) ? "OPEN" : "HALF_OPEN";

                cout << "[METRICS] rate_limit_delay=" << rate_limiter.get_delay_us()/1000 << "ms, "
                     << "429_rate=" << fixed << setprecision(2) << rate_limiter.get_429_rate()*100 << "%, "
                     << "circuit_state=" << cb_state_str << ", "
                     << "total_429s=" << rate_limiter.get_total_429s() << ", "
                     << "dlq_writes=" << dlq.get_total_written() << ", "
                     << "batches_sent=" << batch_metrics.get_total_batches() << ", "
                     << "avg_batch_size=" << fixed << setprecision(1) << batch_metrics.get_avg_batch_size() << ", "
                     << "avg_wait_ms=" << batch_metrics.get_avg_wait_time_us()/1000 << ", "
                     << "max_wait_ms=" << batch_metrics.get_max_wait_time_us()/1000 << endl;
            }
        });
        cout << "[DEBUG] Metrics thread started" << endl;

        cout << "[DEBUG] Creating sender thread pool..." << endl;
        // Sender thread pool
        size_t sender_count = thread::hardware_concurrency();
        if (sender_count == 0) sender_count = 1;
        sender_count = ingestion_endpoint_ok ? min(sender_count, size_t{4}) : 1;

        cout << "[DEBUG] Sender count: " << sender_count << endl;

        vector<thread> senders;
        senders.reserve(sender_count);

        cout << "[DEBUG] Starting sender threads loop..." << endl;
        for (size_t i = 0; i < sender_count; ++i) {
            cout << "[DEBUG] Creating sender thread " << i << endl;
            senders.emplace_back([this, i, &payload_queue, &requeue, &rate_limiter, &circuit_breaker, &dlq]() {
                if (i == 0) {
                    cout << "[DEBUG] Sender thread 0 started with batching (size=" << batch_size
                         << ", timeout=" << batch_timeout_ms << "ms)" << endl;
                }

                // Thread-local batch accumulator
                BatchAccumulator accumulator(batch_size, std::chrono::microseconds(batch_timeout_ms * 1000));
                PayloadWithRetry item;
                size_t processed_count = 0;

                while (true) {
                    // Use timed pop so batch timeout is honored even when queue is slow
                    int pop_result = payload_queue.try_pop_for(item, std::chrono::milliseconds(batch_timeout_ms));

                    if (pop_result == 1) {
                        // Got an item — add to batch
                        accumulator.add(std::move(item));
                    } else if (pop_result == -1) {
                        // Queue closed — flush remaining batch and exit
                        if (!accumulator.empty()) {
                            auto batch = accumulator.take_batch();
                            int64_t wait_time_us = accumulator.get_wait_time_us();
                            post_batch_payload(batch, rate_limiter, circuit_breaker, requeue,
                                             dlq, connection_max_failures, batch_metrics, wait_time_us);
                        }

                        // Drain requeue
                        while (true) {
                            int rq = requeue.try_pop_for(item, std::chrono::milliseconds(100));
                            if (rq != 1) break;
                            post_payload(item, rate_limiter, circuit_breaker, requeue, dlq, connection_max_failures);
                        }
                        break;
                    }
                    // pop_result == 0: timeout — fall through to flush check

                    // Flush batch if ready (size threshold or timeout)
                    if (accumulator.should_flush()) {
                        auto batch = accumulator.take_batch();
                        int64_t wait_time_us = accumulator.get_wait_time_us();
                        post_batch_payload(batch, rate_limiter, circuit_breaker, requeue,
                                         dlq, connection_max_failures, batch_metrics, wait_time_us);
                        processed_count++;
                    }
                }
            });
        }
        cout << "[DEBUG] All sender threads created" << endl;

        cout << "[DEBUG] Starting scheduler thread..." << endl;
        // Scheduler thread
        thread scheduler(&UberSimulator::schedule_events_fixed, this,
                        std::ref(payload_queue), sim_start_ts);
        cout << "[DEBUG] Scheduler thread started, waiting for completion..." << endl;
        scheduler.join();
        cout << "[DEBUG] Scheduler thread completed" << endl;

        // Close requeue after scheduler finishes
        requeue.close();

        for (auto& t : senders) {
            t.join();
        }

        // Stop metrics thread
        metrics_running.store(false);
        metrics_thread.join();

        // Final summary
        cout << ">>> Simulation Completed <<<" << endl;
        cout << ">>> Events sent: " << events_sent.load() << endl;
        cout << ">>> PICKUP: " << pickup_count.load()
             << ", IN_TRANSIT: " << transit_count.load()
             << ", DROPOFF: " << dropoff_count.load() << endl;
        cout << ">>> Circuit breaker trips: " << circuit_breaker.get_total_trips() << endl;
        cout << ">>> Circuit breaker rejects: " << circuit_breaker.get_total_rejects() << endl;
        cout << ">>> Rate limiter 429s: " << rate_limiter.get_total_429s() << endl;
        cout << ">>> DLQ writes: " << dlq.get_total_written() << endl;
        cout << ">>> Batches sent: " << batch_metrics.get_total_batches() << endl;
        cout << ">>> Avg batch size: " << fixed << setprecision(1)
             << batch_metrics.get_avg_batch_size() << endl;
        cout << ">>> Avg wait time: " << batch_metrics.get_avg_wait_time_us()/1000 << "ms" << endl;
        cout << ">>> Max wait time: " << batch_metrics.get_max_wait_time_us()/1000 << "ms" << endl;
    }

private:
    static string format_timestamp(int64_t us) {
        time_t seconds = static_cast<time_t>(us / 1000000LL);
        int64_t micros = us % 1000000LL;
        if (micros < 0) micros += 1000000LL;
        tm tm_value{};
#if defined(__unix__) || defined(__APPLE__)
        gmtime_r(&seconds, &tm_value);
#else
        tm* tm_ptr = gmtime(&seconds);
        if (tm_ptr) tm_value = *tm_ptr;
#endif
        char buffer[32];
        strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%S", &tm_value);
        ostringstream out;
        out << buffer << "." << setw(6) << setfill('0') << micros << "Z";
        return out.str();
    }

    /**
     * FIXED schedule_events - properly handles timing without busy-wait
     */
    void schedule_events_fixed(BoundedQueue<PayloadWithRetry>& payload_queue, int64_t sim_start_ts) {
        cout << "[DEBUG] schedule_events_fixed started" << endl;
        auto wall_start = steady_clock::now();
        size_t processed = 0;

        cout << "[DEBUG] Event queue size: " << event_queue.size() << endl;
        cout << "[DEBUG] Entering main scheduling loop..." << endl;

        while (!event_queue.empty()) {
            if (processed == 0) {
                cout << "[DEBUG] First iteration of scheduling loop" << endl;
            }
            SimulationEvent ev = event_queue.top();

            if (processed == 0) {
                cout << "[DEBUG] First event time: " << format_timestamp(ev.event_time_us) << endl;
            }

            // Calculate current simulation time
            auto now = steady_clock::now();
            double real_elapsed_sec = duration<double>(now - wall_start).count();
            int64_t sim_elapsed_us = static_cast<int64_t>(real_elapsed_sec * playback_speed * 1000000.0);
            int64_t current_sim_ts = sim_start_ts + sim_elapsed_us;

            if (processed == 0) {
                cout << "[DEBUG] current_sim_ts: " << format_timestamp(current_sim_ts)
                     << ", ev.event_time_us: " << format_timestamp(ev.event_time_us) << endl;
                cout << "[DEBUG] Comparison: " << (current_sim_ts >= ev.event_time_us ? "READY" : "WAITING") << endl;
            }

            // Check if it's time to fire this event
            if (current_sim_ts >= ev.event_time_us) {
                if (processed == 0) {
                    cout << "[DEBUG] First event is ready, sending..." << endl;
                }
                // Event is ready - process it
                string payload = build_payload(ev);
                if (processed == 0) {
                    cout << "[DEBUG] Payload built, length: " << payload.length() << endl;
                }
                PayloadWithRetry item{payload, 0};
                if (processed == 0) {
                    cout << "[DEBUG] About to push to payload_queue..." << endl;
                }
                if (!payload_queue.push(std::move(item))) {
                    cout << "[DEBUG] Push failed! Queue closed" << endl;
                    break;  // Queue closed
                }
                if (processed == 0) {
                    cout << "[DEBUG] Push succeeded!" << endl;
                }
                event_queue.pop();
                events_sent.fetch_add(1, memory_order_relaxed);
                
                // Update stats
                switch (ev.type) {
                    case EventType::PICKUP: pickup_count.fetch_add(1); break;
                    case EventType::IN_TRANSIT: transit_count.fetch_add(1); break;
                    case EventType::DROPOFF: dropoff_count.fetch_add(1); break;
                }

                // Lazily schedule IN_TRANSIT events to avoid precomputing all segments.
                if (ev.type == EventType::PICKUP || ev.type == EventType::IN_TRANSIT) {
                    int64_t next_time = ev.event_time_us + TRANSIT_UPDATE_INTERVAL_US;
                    if (next_time < ev.raw_data->tpep_dropoff_datetime) {
                        event_queue.push({EventType::IN_TRANSIT, next_time, ev.raw_data});
                    }
                }
                
                processed++;
                
                // Debug logging every 10000 events
                if (debug_timing && processed % 10000 == 0) {
                    lock_guard<mutex> lock(log_mu);
                    cout << "[DEBUG] Processed " << processed 
                         << " events, sim_time=" << format_timestamp(current_sim_ts)
                         << ", event_time=" << format_timestamp(ev.event_time_us) << endl;
                }
            } else {
                // Event not ready yet - calculate wait time
                int64_t wait_us = ev.event_time_us - current_sim_ts;
                double wait_real_us = static_cast<double>(wait_us) / playback_speed;
                
                // FIX: Always sleep, but use different strategies based on wait time
                if (wait_real_us > 10000.0) {
                    // Long wait (>10ms): sleep for most of it, wake up early to avoid oversleep
                    auto sleep_duration = microseconds(static_cast<int64_t>(wait_real_us * 0.9));
                    this_thread::sleep_for(sleep_duration);
                } else if (wait_real_us > 100.0) {
                    // Medium wait (0.1ms - 10ms): sleep for the exact duration
                    auto sleep_duration = microseconds(static_cast<int64_t>(wait_real_us));
                    this_thread::sleep_for(sleep_duration);
                } else {
                    // Short wait (<0.1ms): yield to avoid busy-wait, then recheck
                    this_thread::yield();
                }
            }
        }

        payload_queue.close();
        
        if (debug_timing) {
            lock_guard<mutex> lock(log_mu);
            cout << "[DEBUG] Scheduler finished. Total processed: " << processed << endl;
        }
    }

    string build_payload(const SimulationEvent& ev) {
        stringstream json;
        const auto* d = ev.raw_data;

        json << "{";
        if (ev.type == EventType::PICKUP) {
            auto coords = geo_service.get_coords(d->PULocationID);
            json << "\"event\": \"PICKUP\", "
                 << "\"trip_id\": " << d->trip_id << ", "
                 << "\"PULocationID\": " << d->PULocationID << ", "
                 << "\"lat\": " << fixed << setprecision(6) << coords.first << ", "
                 << "\"lon\": " << coords.second << ", "
                 << "\"VendorID\": " << d->VendorID << ", "
                 << "\"passenger_count\": " << d->passenger_count << ", "
                 << "\"ts\": \"" << format_timestamp(d->tpep_pickup_datetime) << "\"";
        } 
        else if (ev.type == EventType::IN_TRANSIT) {
            auto coords = geo_service.interpolate(
                d->PULocationID, d->DOLocationID,
                d->tpep_pickup_datetime, d->tpep_dropoff_datetime,
                ev.event_time_us
            );
            json << "\"event\": \"IN_TRANSIT\", "
                 << "\"trip_id\": " << d->trip_id << ", "
                 << "\"lat\": " << fixed << setprecision(6) << coords.first << ", "
                 << "\"lon\": " << coords.second << ", "
                 << "\"ts\": \"" << format_timestamp(ev.event_time_us) << "\"";
        } 
        else { // DROPOFF
            auto coords = geo_service.get_coords(d->DOLocationID);
            json << "\"event\": \"DROPOFF\", "
                 << "\"trip_id\": " << d->trip_id << ", "
                 << "\"DOLocationID\": " << d->DOLocationID << ", "
                 << "\"lat\": " << fixed << setprecision(6) << coords.first << ", "
                 << "\"lon\": " << coords.second << ", "
                 << "\"fare_amount\": " << d->fare_amount << ", "
                 << "\"total_amount\": " << d->total_amount << ", "
                 << "\"trip_distance\": " << d->trip_distance << ", "
                 << "\"RatecodeID\": " << d->RatecodeID << ", "
                 << "\"payment_type\": " << d->payment_type << ", "
                 << "\"extra\": " << d->extra << ", "
                 << "\"mta_tax\": " << d->mta_tax << ", "
                 << "\"tip_amount\": " << d->tip_amount << ", "
                 << "\"tolls_amount\": " << d->tolls_amount << ", "
                 << "\"improvement_surcharge\": " << d->improvement_surcharge << ", "
                 << "\"congestion_surcharge\": " << d->congestion_surcharge << ", "
                 << "\"Airport_fee\": " << d->Airport_fee << ", "
                 << "\"ts\": \"" << format_timestamp(d->tpep_dropoff_datetime) << "\"";
        }
        json << "}";

        return json.str();
    }

    void post_payload(PayloadWithRetry& item, RateLimiter& rate_limiter,
                     CircuitBreaker& circuit_breaker,
                     BoundedQueue<PayloadWithRetry>& requeue,
                     DeadLetterQueue& dlq,
                     size_t max_failures) {
        // 1. Check circuit breaker
        if (!circuit_breaker.allow_request()) {
            // Circuit open - requeue if possible
            if (item.can_retry()) {
                item.increment_retry();
                if (!requeue.push(std::move(item))) {
                    // Requeue full -> DLQ
                    dlq.write(item.payload, item.retry_count);
                    lock_guard<mutex> lock(log_mu);
                    cerr << "[DLQ] Circuit open, requeue full, wrote to DLQ" << endl;
                }
            } else {
                // Max retries exceeded -> DLQ
                dlq.write(item.payload, item.retry_count);
                lock_guard<mutex> lock(log_mu);
                cerr << "[DLQ] Max retries exceeded, wrote to DLQ" << endl;
            }
            return;
        }

        // 2. Apply rate limiting delay
        int64_t delay_us = rate_limiter.get_delay_us();
        if (delay_us > 0) {
            this_thread::sleep_for(microseconds(delay_us));
        }

        // 3. Send request (or log if no endpoint)
        if (!ingestion_endpoint_ok) {
            cout << "[NET] " << item.payload << endl;
            rate_limiter.record_response(200);
            circuit_breaker.record_result(200);
            return;
        }

        int status_code = 0;
        bool success = http_post_json(ingestion_endpoint, item.payload, status_code, max_failures);

        // 4. Record metrics
        rate_limiter.record_response(status_code);
        circuit_breaker.record_result(status_code);

        // 5. Handle failure
        if (!success) {
            // Requeue for: 429 (rate limit), 500+ (server error), 0 (socket error)
            if (status_code == 429 || status_code >= 500 || status_code == 0) {
                if (item.can_retry()) {
                    item.increment_retry();
                    if (!requeue.push(std::move(item))) {
                        // Requeue full -> DLQ
                        dlq.write(item.payload, item.retry_count);
                        lock_guard<mutex> lock(log_mu);
                        cerr << "[DLQ] Requeue full after failure, wrote to DLQ" << endl;
                    }
                } else {
                    // Max retries exceeded -> DLQ
                    dlq.write(item.payload, item.retry_count);
                    lock_guard<mutex> lock(log_mu);
                    cerr << "[DLQ] Max retries exceeded after failure, wrote to DLQ" << endl;
                }
            } else {
                // Client error (4xx except 429) -> log and drop (our fault)
                lock_guard<mutex> lock(log_mu);
                cerr << "[NET] Client error (dropped): status=" << status_code << endl;
            }
        }
    }

    // Batch version: sends multiple events in a single request
    void post_batch_payload(std::vector<PayloadWithRetry>& batch,
                           RateLimiter& rate_limiter,
                           CircuitBreaker& circuit_breaker,
                           BoundedQueue<PayloadWithRetry>& requeue,
                           DeadLetterQueue& dlq,
                           size_t max_failures,
                           BatchMetrics& batch_metrics,
                           int64_t wait_time_us) {
        if (batch.empty()) return;

        // 1. Check circuit breaker
        if (!circuit_breaker.allow_request()) {
            // Circuit open - requeue all events individually
            for (auto& item : batch) {
                if (item.can_retry()) {
                    item.increment_retry();
                    if (!requeue.push(std::move(item))) {
                        dlq.write(item.payload, item.retry_count);
                    }
                } else {
                    dlq.write(item.payload, item.retry_count);
                }
            }
            return;
        }

        // 2. Apply rate limiting delay (once per batch)
        int64_t delay_us = rate_limiter.get_delay_us();
        if (delay_us > 0) {
            this_thread::sleep_for(microseconds(delay_us));
        }

        // 3. Send batch request (or log if no endpoint)
        if (!ingestion_endpoint_ok) {
            // Stdout fallback - print each event
            for (const auto& item : batch) {
                cout << "[NET] " << item.payload << endl;
            }
            rate_limiter.record_response(200);
            circuit_breaker.record_result(200);
            batch_metrics.record_batch(batch.size(), wait_time_us);
            return;
        }

        int status_code = 0;
        bool success = http_post_json_batch(ingestion_endpoint, batch,
                                            status_code, max_failures);

        // 4. Record metrics
        rate_limiter.record_response(status_code);
        circuit_breaker.record_result(status_code);

        if (success) {
            batch_metrics.record_batch(batch.size(), wait_time_us);
            return;
        }

        // 5. Handle batch failure
        if (status_code == 400) {
            // Bad request - likely one bad event in batch
            // Split and retry individually to isolate bad event
            lock_guard<mutex> lock(log_mu);
            cerr << "[BATCH] 400 on batch (size=" << batch.size()
                 << "), splitting and retrying individually" << endl;

            for (auto& item : batch) {
                // Retry as individual event using existing post_payload
                post_payload(item, rate_limiter, circuit_breaker, requeue,
                           dlq, max_failures);
            }
        } else if (status_code == 429 || status_code >= 500 || status_code == 0) {
            // Retryable error - requeue all events individually
            for (auto& item : batch) {
                if (item.can_retry()) {
                    item.increment_retry();
                    if (!requeue.push(std::move(item))) {
                        dlq.write(item.payload, item.retry_count);
                    }
                } else {
                    dlq.write(item.payload, item.retry_count);
                }
            }
        } else {
            // Other client errors (401, 403, 404, etc.) - drop all
            lock_guard<mutex> lock(log_mu);
            cerr << "[BATCH] Client error on batch: status=" << status_code
                 << ", size=" << batch.size() << " (dropped)" << endl;
        }
    }
};

// ==========================================
// 4. Parquet Loader
// ==========================================

class ParquetLoader {
public:
    static void load_from_directory(const string& root_path, UberSimulator& sim, const DateRange& range) {
        if (!fs::exists(root_path)) {
            cerr << "Error: Directory not found -> " << root_path << endl;
            return;
        }

        vector<fs::path> files;
        for (const auto& entry : fs::recursive_directory_iterator(root_path)) {
            if (!entry.is_regular_file() || entry.path().extension() != ".parquet") {
                continue;
            }
            if (!is_in_date_range(entry.path().filename().string(), range)) {
                continue;
            }
            files.push_back(entry.path());
        }

        if (files.empty()) {
            cout << "[Loader] No parquet files found in range." << endl;
            return;
        }

        sort(files.begin(), files.end());

        const size_t max_workers = 2;
        size_t worker_count = thread::hardware_concurrency();
        if (worker_count == 0) worker_count = 1;
        worker_count = min(worker_count, max_workers);

        atomic<size_t> next_index{0};
        mutex log_mu;
        vector<thread> workers;
        workers.reserve(worker_count);

        for (size_t i = 0; i < worker_count; ++i) {
            workers.emplace_back([&]() {
                while (true) {
                    size_t idx = next_index.fetch_add(1);
                    if (idx >= files.size()) break;
                    const auto& path = files[idx];
                    {
                        lock_guard<mutex> lock(log_mu);
                        cout << "[Loader] Reading: " << path.filename() << "... ";
                    }
                    load_file(path.string(), sim);
                    {
                        lock_guard<mutex> lock(log_mu);
                        cout << "Done." << endl;
                    }
                }
            });
        }

        for (auto& t : workers) {
            t.join();
        }
    }

private:
    static bool extract_year_month(const string& name, int& year, int& month) {
        for (size_t i = 0; i + 6 < name.size(); ++i) {
            if (!isdigit(static_cast<unsigned char>(name[i])) ||
                !isdigit(static_cast<unsigned char>(name[i + 1])) ||
                !isdigit(static_cast<unsigned char>(name[i + 2])) ||
                !isdigit(static_cast<unsigned char>(name[i + 3])) ||
                name[i + 4] != '-' ||
                !isdigit(static_cast<unsigned char>(name[i + 5])) ||
                !isdigit(static_cast<unsigned char>(name[i + 6]))) {
                continue;
            }
            int y = stoi(name.substr(i, 4));
            int m = stoi(name.substr(i + 5, 2));
            if (m < 1 || m > 12) continue;
            year = y;
            month = m;
            return true;
        }
        return false;
    }

    static bool is_in_date_range(const string& name, const DateRange& range) {
        int year = 0, month = 0;
        if (!extract_year_month(name, year, month)) return false;
        int value = year * 12 + month;
        int start = range.start_year * 12 + range.start_month;
        int end = range.end_year * 12 + range.end_month;
        return value >= start && value <= end;
    }

    static int64_t timestamp_value_us(const shared_ptr<arrow::TimestampArray>& arr, int64_t i) {
        int64_t value = arr->Value(i);
        auto ts_type = static_pointer_cast<arrow::TimestampType>(arr->type());
        switch (ts_type->unit()) {
            case arrow::TimeUnit::SECOND: return value * 1000000LL;
            case arrow::TimeUnit::MILLI: return value * 1000LL;
            case arrow::TimeUnit::MICRO: return value;
            case arrow::TimeUnit::NANO: return value / 1000LL;
        }
        return value;
    }

    static int64_t normalize_epoch_us(int64_t value) {
        if (value == 0) return 0;
        int64_t abs_value = llabs(value);
        // Already in reasonable microseconds range (2010-2030)
        const int64_t min_us = 1262304000000000LL; // 2010-01-01
        const int64_t max_us = 1893456000000000LL; // 2030-01-01
        if (abs_value >= min_us && abs_value <= max_us) {
            return value;  // Already microseconds
        }
        // Try to detect and convert
        if (abs_value < 100000000000LL) {
            return value * 1000000LL; // seconds -> us
        }
        if (abs_value < 100000000000000LL) {
            return value * 1000LL; // millis -> us
        }
        if (abs_value < 100000000000000000LL) {
            return value; // micros
        }
        return value / 1000LL; // nanos -> us
    }

    static int64_t first_nonzero_in_array(const shared_ptr<arrow::Array>& arr) {
        if (!arr || arr->length() == 0) return 0;
        for (int64_t i = 0; i < arr->length(); ++i) {
            int64_t value = get_int64_value(arr, i, 0);
            if (value != 0) return value;
        }
        return get_int64_value(arr, 0, 0);
    }

    static bool is_reasonable_epoch_us(int64_t value) {
        const int64_t min = 1262304000000000LL; // 2010-01-01
        const int64_t max = 1893456000000000LL; // 2030-01-01
        return value >= min && value <= max;
    }

    static string format_epoch_us(int64_t value) {
        if (value <= 0) return "n/a";
        time_t seconds = static_cast<time_t>(value / 1000000LL);
        tm tm_value{};
#if defined(__unix__) || defined(__APPLE__)
        if (gmtime_r(&seconds, &tm_value) == nullptr) return "n/a";
#else
        tm* tm_ptr = gmtime(&seconds);
        if (!tm_ptr) return "n/a";
        tm_value = *tm_ptr;
#endif
        char buffer[32];
        if (strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%SZ", &tm_value) == 0) {
            return "n/a";
        }
        return string(buffer);
    }

    static void log_timestamp_sample(const string& path,
                                     const string& label,
                                     const shared_ptr<arrow::Array>& column) {
        if (!column || column->length() == 0) return;
        static mutex log_mu;
        int64_t raw = first_nonzero_in_array(column);
        int64_t normalized = normalize_epoch_us(raw);
        string type = column ? column->type()->ToString() : "missing";
        string iso = format_epoch_us(normalized);
        lock_guard<mutex> lock(log_mu);
        cout << "[Loader] Timestamp sample (" << label << ") file="
             << fs::path(path).filename()
             << " type=" << type
             << " raw=" << raw
             << " normalized_us=" << normalized
             << " iso=" << iso;
        if (!is_reasonable_epoch_us(normalized)) {
            cout << " range=OUT_OF_RANGE";
        }
        cout << endl;
    }

    static int64_t get_int64_value(const shared_ptr<arrow::Array>& arr, int64_t i, int64_t default_val) {
        if (!arr || i >= arr->length() || arr->IsNull(i)) return default_val;
        switch (arr->type_id()) {
            case arrow::Type::INT64:
                return static_pointer_cast<arrow::Int64Array>(arr)->Value(i);
            case arrow::Type::INT32:
                return static_pointer_cast<arrow::Int32Array>(arr)->Value(i);
            case arrow::Type::INT16:
                return static_pointer_cast<arrow::Int16Array>(arr)->Value(i);
            case arrow::Type::INT8:
                return static_pointer_cast<arrow::Int8Array>(arr)->Value(i);
            case arrow::Type::UINT64:
                return static_pointer_cast<arrow::UInt64Array>(arr)->Value(i);
            case arrow::Type::UINT32:
                return static_pointer_cast<arrow::UInt32Array>(arr)->Value(i);
            case arrow::Type::UINT16:
                return static_pointer_cast<arrow::UInt16Array>(arr)->Value(i);
            case arrow::Type::UINT8:
                return static_pointer_cast<arrow::UInt8Array>(arr)->Value(i);
            case arrow::Type::DOUBLE:
                return static_cast<int64_t>(static_pointer_cast<arrow::DoubleArray>(arr)->Value(i));
            case arrow::Type::FLOAT:
                return static_cast<int64_t>(static_pointer_cast<arrow::FloatArray>(arr)->Value(i));
            case arrow::Type::TIMESTAMP:
                return timestamp_value_us(static_pointer_cast<arrow::TimestampArray>(arr), i);
            case arrow::Type::DATE64:
                return static_pointer_cast<arrow::Date64Array>(arr)->Value(i) * 1000LL;
            case arrow::Type::DATE32:
                return static_pointer_cast<arrow::Date32Array>(arr)->Value(i) * 86400000000LL;
            case arrow::Type::DICTIONARY: {
                auto dict_arr = static_pointer_cast<arrow::DictionaryArray>(arr);
                if (dict_arr->IsNull(i)) return default_val;
                int64_t dict_index = dict_arr->GetValueIndex(i);
                return get_int64_value(dict_arr->dictionary(), dict_index, default_val);
            }
            default:
                return default_val;
        }
    }

    static double get_double_value(const shared_ptr<arrow::Array>& arr, int64_t i, double default_val) {
        if (!arr || i >= arr->length() || arr->IsNull(i)) return default_val;
        switch (arr->type_id()) {
            case arrow::Type::DOUBLE:
                return static_pointer_cast<arrow::DoubleArray>(arr)->Value(i);
            case arrow::Type::FLOAT:
                return static_pointer_cast<arrow::FloatArray>(arr)->Value(i);
            case arrow::Type::INT64:
                return static_cast<double>(static_pointer_cast<arrow::Int64Array>(arr)->Value(i));
            case arrow::Type::INT32:
                return static_cast<double>(static_pointer_cast<arrow::Int32Array>(arr)->Value(i));
            case arrow::Type::INT16:
                return static_cast<double>(static_pointer_cast<arrow::Int16Array>(arr)->Value(i));
            case arrow::Type::INT8:
                return static_cast<double>(static_pointer_cast<arrow::Int8Array>(arr)->Value(i));
            case arrow::Type::UINT64:
                return static_cast<double>(static_pointer_cast<arrow::UInt64Array>(arr)->Value(i));
            case arrow::Type::UINT32:
                return static_cast<double>(static_pointer_cast<arrow::UInt32Array>(arr)->Value(i));
            case arrow::Type::UINT16:
                return static_cast<double>(static_pointer_cast<arrow::UInt16Array>(arr)->Value(i));
            case arrow::Type::UINT8:
                return static_cast<double>(static_pointer_cast<arrow::UInt8Array>(arr)->Value(i));
            case arrow::Type::DICTIONARY: {
                auto dict_arr = static_pointer_cast<arrow::DictionaryArray>(arr);
                if (dict_arr->IsNull(i)) return default_val;
                int64_t dict_index = dict_arr->GetValueIndex(i);
                return get_double_value(dict_arr->dictionary(), dict_index, default_val);
            }
            default:
                return default_val;
        }
    }

    static string get_string_value(const shared_ptr<arrow::Array>& arr, int64_t i) {
        if (!arr || i >= arr->length() || arr->IsNull(i)) return "";
        switch (arr->type_id()) {
            case arrow::Type::STRING:
                return static_pointer_cast<arrow::StringArray>(arr)->GetString(i);
            case arrow::Type::LARGE_STRING:
                return static_pointer_cast<arrow::LargeStringArray>(arr)->GetString(i);
            case arrow::Type::DICTIONARY: {
                auto dict_arr = static_pointer_cast<arrow::DictionaryArray>(arr);
                if (dict_arr->IsNull(i)) return "";
                int64_t dict_index = dict_arr->GetValueIndex(i);
                return get_string_value(dict_arr->dictionary(), dict_index);
            }
            default:
                return "";
        }
    }

    static shared_ptr<arrow::Array> get_column(const shared_ptr<arrow::RecordBatch>& batch,
                                               const string& name) {
        if (!batch) return nullptr;
        int idx = batch->schema()->GetFieldIndex(name);
        if (idx < 0) return nullptr;
        return batch->column(idx);
    }

    static size_t process_record_batch(const string& path,
                                       const shared_ptr<arrow::RecordBatch>& batch,
                                       UberSimulator& sim,
                                       bool& logged_samples) {
        if (!batch || batch->num_rows() == 0) return 0;

        auto pu_times = get_column(batch, "tpep_pickup_datetime");
        auto do_times = get_column(batch, "tpep_dropoff_datetime");

        if (!pu_times || !do_times) {
            cerr << "[Loader] Missing required timestamps in: " << path << endl;
            return 0;
        }

        if (!logged_samples) {
            log_timestamp_sample(path, "pickup", pu_times);
            log_timestamp_sample(path, "dropoff", do_times);
            logged_samples = true;
        }

        auto trip_ids = get_column(batch, "trip_id");
        auto pu_locs = get_column(batch, "PULocationID");
        auto do_locs = get_column(batch, "DOLocationID");
        auto vendors = get_column(batch, "VendorID");
        auto ratecodes = get_column(batch, "RatecodeID");
        auto payment_types = get_column(batch, "payment_type");
        auto store_flags = get_column(batch, "store_and_fwd_flag");
        auto passengers = get_column(batch, "passenger_count");

        auto congestion = get_column(batch, "congestion_surcharge");
        auto extras = get_column(batch, "extra");
        auto fares = get_column(batch, "fare_amount");
        auto improvements = get_column(batch, "improvement_surcharge");
        auto mta_taxes = get_column(batch, "mta_tax");
        auto tips = get_column(batch, "tip_amount");
        auto tolls = get_column(batch, "tolls_amount");
        auto totals = get_column(batch, "total_amount");
        auto dists = get_column(batch, "trip_distance");
        auto airport_fees = get_column(batch, "Airport_fee");

        if (!trip_ids) {
            cerr << "[Loader] Missing trip_id column in: " << path << endl;
            return 0;
        }

        vector<RawTripData> batch_data;
        batch_data.reserve(batch->num_rows());

        for (int64_t i = 0; i < batch->num_rows(); ++i) {
            RawTripData data;
            data.trip_id = get_int64_value(trip_ids, i, 0);
            data.tpep_pickup_datetime = normalize_epoch_us(get_int64_value(pu_times, i, 0));
            data.tpep_dropoff_datetime = normalize_epoch_us(get_int64_value(do_times, i, 0));
            data.PULocationID = get_int64_value(pu_locs, i, 0);
            data.DOLocationID = get_int64_value(do_locs, i, 0);

            data.VendorID = get_int64_value(vendors, i, 0);
            data.RatecodeID = get_int64_value(ratecodes, i, 0);
            data.payment_type = get_int64_value(payment_types, i, 0);
            data.store_and_fwd_flag = get_string_value(store_flags, i);
            data.passenger_count = get_int64_value(passengers, i, 0);

            data.congestion_surcharge = get_double_value(congestion, i, 0.0);
            data.extra = get_double_value(extras, i, 0.0);
            data.fare_amount = get_double_value(fares, i, 0.0);
            data.improvement_surcharge = get_double_value(improvements, i, 0.0);
            data.mta_tax = get_double_value(mta_taxes, i, 0.0);
            data.tip_amount = get_double_value(tips, i, 0.0);
            data.tolls_amount = get_double_value(tolls, i, 0.0);
            data.total_amount = get_double_value(totals, i, 0.0);
            data.trip_distance = get_double_value(dists, i, 0.0);
            data.Airport_fee = get_double_value(airport_fees, i, 0.0);

            batch_data.push_back(std::move(data));
        }

        size_t ingested = batch_data.size();
        if (ingested > 0) {
            sim.ingest_trips(std::move(batch_data));
        }
        return ingested;
    }

    static void load_file(const string& path, UberSimulator& sim) {
        arrow::Status st;
        arrow::MemoryPool* pool = arrow::default_memory_pool();

        auto file_res = arrow::io::ReadableFile::Open(path);
        if (!file_res.ok()) {
            cerr << "[Loader] Failed to open " << path << ": " << file_res.status().ToString() << endl;
            return;
        }
        
        std::unique_ptr<parquet::arrow::FileReader> reader;
        st = parquet::arrow::OpenFile(*file_res, pool, &reader);
        if (!st.ok()) {
            cerr << "[Loader] Failed to open Parquet reader for " << path << ": " << st.ToString() << endl;
            return;
        }

        int row_groups = reader->num_row_groups();
        size_t ingested_total = 0;
        int64_t row_total = 0;
        bool logged_samples = false;

        for (int rg = 0; rg < row_groups; ++rg) {
            std::shared_ptr<arrow::Table> table;
            st = reader->ReadRowGroups({rg}, &table);
            if (!st.ok()) {
                cerr << "[Loader] Failed to read row group for " << path << ": " << st.ToString() << endl;
                continue;
            }
            if (!table) continue;
            row_total += table->num_rows();

            arrow::TableBatchReader batch_reader(*table);
            batch_reader.set_chunksize(16384);
            while (true) {
                std::shared_ptr<arrow::RecordBatch> batch;
                st = batch_reader.ReadNext(&batch);
                if (!st.ok()) {
                    cerr << "[Loader] Failed to read batch for " << path << ": " << st.ToString() << endl;
                    break;
                }
                if (!batch) break;
                ingested_total += process_record_batch(path, batch, sim, logged_samples);
            }
        }

        cout << "[Loader] " << path << " rows=" << row_total
             << " ingested=" << ingested_total << endl;
    }
};

// ==========================================
// 5. Main Entry
// ==========================================

int main(int argc, char** argv) {
#if defined(__unix__) || defined(__APPLE__)
    // Prevent SIGPIPE from killing the process when nginx/ingestor resets connection
    signal(SIGPIPE, SIG_IGN);
#endif

    string config_path = DEFAULT_CONFIG_PATH;
    if (argc > 1) {
        config_path = argv[1];
    }
    SimulationConfig config = load_config(config_path);

    UberSimulator sim(config);

    cout << "=== Uber Data Generator Initializing ===" << endl;
    cout << "=== Playback Speed: " << config.playback_speed << "x ===" << endl;
    ParquetLoader::load_from_directory(config.data_path, sim, config.range);

    sim.run();

    return 0;
}

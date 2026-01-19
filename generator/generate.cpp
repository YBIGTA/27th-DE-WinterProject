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

struct DateRange {
    int start_year;
    int start_month;
    int end_year;
    int end_month;
};

struct SimulationConfig {
    double playback_speed = 100.0;
    DateRange range{2020, 1, 2024, 4};
    string data_path = "../data/taxi_data_preprocessed";
    string ingestion_url = "http://localhost:8080/ingest";
    bool debug_timing = false;  // NEW: Enable timing debug logs
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

static SimulationConfig load_config(const string& path) {
    SimulationConfig config;
    ifstream file(path);
    if (!file.is_open()) {
        cerr << "[Config] Using defaults (file not found: " << path << ")" << endl;
        const char* env_url = getenv("INGEST_URL");
        if (env_url && *env_url) config.ingestion_url = env_url;
        return config;
    }

    string line;
    while (getline(file, line)) {
        string trimmed = trim_copy(line);
        if (trimmed.empty() || trimmed[0] == '#') continue;
        size_t eq = trimmed.find('=');
        if (eq == string::npos) continue;

        string key = to_lower_copy(trim_copy(trimmed.substr(0, eq)));
        string value = trim_copy(trimmed.substr(eq + 1));
        if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
            value = value.substr(1, value.size() - 2);
        }

        if (key == "playback_speed") {
            try { config.playback_speed = stod(value); } catch (...) {}
        } else if (key == "start" || key == "start_date") {
            int year = 0, month = 0;
            if (parse_year_month_value(value, year, month)) {
                config.range.start_year = year;
                config.range.start_month = month;
            }
        } else if (key == "end" || key == "end_date") {
            int year = 0, month = 0;
            if (parse_year_month_value(value, year, month)) {
                config.range.end_year = year;
                config.range.end_month = month;
            }
        } else if (key == "data_path") {
            if (!value.empty()) config.data_path = value;
        } else if (key == "ingestion_url" || key == "ingest_url") {
            if (!value.empty()) config.ingestion_url = value;
        } else if (key == "debug_timing") {
            config.debug_timing = (value == "true" || value == "1");
        }
    }

    const char* env_url = getenv("INGEST_URL");
    if (env_url && *env_url) config.ingestion_url = env_url;

    return config;
}

class BoundedQueue {
public:
    explicit BoundedQueue(size_t capacity) : capacity_(capacity) {}

    bool push(string item) {
        unique_lock<mutex> lock(mu_);
        not_full_.wait(lock, [&] { return closed_ || queue_.size() < capacity_; });
        if (closed_) return false;
        queue_.push_back(std::move(item));
        not_empty_.notify_one();
        return true;
    }

    bool pop(string& out) {
        unique_lock<mutex> lock(mu_);
        not_empty_.wait(lock, [&] { return closed_ || !queue_.empty(); });
        if (queue_.empty()) return false;
        out = std::move(queue_.front());
        queue_.pop_front();
        not_full_.notify_one();
        return true;
    }

    void close() {
        lock_guard<mutex> lock(mu_);
        closed_ = true;
        not_empty_.notify_all();
        not_full_.notify_all();
    }

private:
    size_t capacity_;
    deque<string> queue_;
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

static bool read_http_response(int sock) {
    string buffer;
    buffer.reserve(2048);
    size_t header_end = string::npos;
    while (header_end == string::npos) {
        if (!recv_more(sock, buffer)) return false;
        header_end = buffer.find("\r\n\r\n");
        if (buffer.size() > 65536) return false;
    }

    string header = buffer.substr(0, header_end);
    size_t body_offset = header_end + 4;
    string pending = buffer.substr(body_offset);

    int64_t content_length = -1;
    bool chunked = false;
    istringstream header_stream(header);
    string line;
    getline(header_stream, line);
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

    bool post_json(const string& payload) {
        if (!ensure_connected()) return false;

        string request = "POST " + endpoint_.path + " HTTP/1.1\r\n";
        request += "Host: " + endpoint_.host + "\r\n";
        request += "Content-Type: application/json\r\n";
        request += "Content-Length: " + to_string(payload.size()) + "\r\n";
        request += "Connection: keep-alive\r\n\r\n";
        request += payload;

        if (!send_all(sock_, request.data(), request.size())) {
            close_socket();
            return false;
        }

        if (!read_http_response(sock_)) {
            close_socket();
            return false;
        }

        return true;
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

static bool http_post_json(const HttpEndpoint& endpoint, const string& payload) {
    thread_local HttpConnection conn;
    if (!conn.matches(endpoint)) {
        conn.reset(endpoint);
    }
    if (conn.post_json(payload)) return true;
    conn.reset(endpoint);
    return conn.post_json(payload);
}
#else
static bool http_post_json(const HttpEndpoint&, const string&) {
    return false;
}
#endif


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
    explicit GeoService(const string& csv_path = "../data/taxi_zones/taxi_zone_median_coords.csv") {
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

public:
    UberSimulator(double speed, string url, bool debug = false) 
        : playback_speed(speed), ingestion_url(std::move(url)), debug_timing(debug) {
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

        BoundedQueue payload_queue(PAYLOAD_QUEUE_CAPACITY);

        size_t sender_count = thread::hardware_concurrency();
        if (sender_count == 0) sender_count = 1;
        sender_count = ingestion_endpoint_ok ? sender_count * 2 : 1;

        vector<thread> senders;
        senders.reserve(sender_count);
        for (size_t i = 0; i < sender_count; ++i) {
            senders.emplace_back([this, &payload_queue]() {
                string payload;
                while (payload_queue.pop(payload)) {
                    post_payload(payload);
                }
            });
        }

        thread scheduler(&UberSimulator::schedule_events_fixed, this, std::ref(payload_queue), sim_start_ts);
        scheduler.join();
        
        for (auto& t : senders) {
            t.join();
        }
        
        cout << ">>> Simulation Completed <<<" << endl;
        cout << ">>> Events sent: " << events_sent.load() << endl;
        cout << ">>> PICKUP: " << pickup_count.load() 
             << ", IN_TRANSIT: " << transit_count.load() 
             << ", DROPOFF: " << dropoff_count.load() << endl;
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
    void schedule_events_fixed(BoundedQueue& payload_queue, int64_t sim_start_ts) {
        auto wall_start = steady_clock::now();
        size_t processed = 0;
        
        while (!event_queue.empty()) {
            SimulationEvent ev = event_queue.top();

            // Calculate current simulation time
            auto now = steady_clock::now();
            double real_elapsed_sec = duration<double>(now - wall_start).count();
            int64_t sim_elapsed_us = static_cast<int64_t>(real_elapsed_sec * playback_speed * 1000000.0);
            int64_t current_sim_ts = sim_start_ts + sim_elapsed_us;

            // Check if it's time to fire this event
            if (current_sim_ts >= ev.event_time_us) {
                // Event is ready - process it
                string payload = build_payload(ev);
                if (!payload_queue.push(std::move(payload))) {
                    break;  // Queue closed
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

    void post_payload(const string& payload) {
        if (!ingestion_endpoint_ok) {
            cout << "[NET] " << payload << endl;
            return;
        }
        if (!http_post_json(ingestion_endpoint, payload)) {
            lock_guard<mutex> lock(log_mu);
            cerr << "[NET] POST failed: " << ingestion_url << endl;
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
    string config_path = "config.txt";
    if (argc > 1) {
        config_path = argv[1];
    }
    SimulationConfig config = load_config(config_path);

    UberSimulator sim(config.playback_speed, config.ingestion_url, config.debug_timing);

    cout << "=== Uber Data Generator Initializing ===" << endl;
    cout << "=== Playback Speed: " << config.playback_speed << "x ===" << endl;
    ParquetLoader::load_from_directory(config.data_path, sim, config.range);

    sim.run();

    return 0;
}

/**
 * Uber-Style Traffic Simulator
 * * Features:
 * 1. Reads Parquet files from a directory structure using Apache Arrow.
 * 2. Generates unique Trip IDs.
 * 3. Simulates 3 trip states: PICKUP -> IN_TRANSIT (GPS Interpolated) -> DROPOFF.
 * 4. Replays events based on historical timestamps with n-speed playback control.
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
#include <unistd.h>
#endif

using namespace std;
using namespace std::chrono;
namespace fs = std::filesystem;

// ==========================================
// 1. Data Structures & Enums
// ==========================================

// Parquet Row Data Wrapper
struct RawTripData {
    string trip_id; // Generated UUID
    
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
    
    int64_t tpep_pickup_datetime;  // Microseconds
    int64_t tpep_dropoff_datetime; // Microseconds
};

enum class EventType { PICKUP, IN_TRANSIT, DROPOFF };

struct SimulationEvent {
    EventType type;
    int64_t event_time_us; // Simulation Trigger Time
    
    // Pointer to raw data to save memory
    const RawTripData* raw_data;
    
    // For IN_TRANSIT
    double current_lat;
    double current_lon;

    // Priority Queue: Smallest time pops first
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
            try {
                config.playback_speed = stod(value);
            } catch (...) {
            }
        } else if (key == "start" || key == "start_date") {
            int year = 0;
            int month = 0;
            if (parse_year_month_value(value, year, month)) {
                config.range.start_year = year;
                config.range.start_month = month;
            }
        } else if (key == "end" || key == "end_date") {
            int year = 0;
            int month = 0;
            if (parse_year_month_value(value, year, month)) {
                config.range.end_year = year;
                config.range.end_month = month;
            }
        } else if (key == "data_path") {
            if (!value.empty()) config.data_path = value;
        } else if (key == "ingestion_url" || key == "ingest_url") {
            if (!value.empty()) config.ingestion_url = value;
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

static bool http_post_json(const HttpEndpoint& endpoint, const string& payload) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* res = nullptr;
    if (getaddrinfo(endpoint.host.c_str(), endpoint.port.c_str(), &hints, &res) != 0) {
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

    string request = "POST " + endpoint.path + " HTTP/1.1\r\n";
    request += "Host: " + endpoint.host + "\r\n";
    request += "Content-Type: application/json\r\n";
    request += "Content-Length: " + to_string(payload.size()) + "\r\n";
    request += "Connection: close\r\n\r\n";
    request += payload;

    bool ok = send_all(sock, request.data(), request.size());
    if (ok) {
        char buffer[512];
        while (recv(sock, buffer, sizeof(buffer), 0) > 0) {
        }
    }
    close(sock);
    return ok;
}
#else
static bool http_post_json(const HttpEndpoint&, const string&) {
    return false;
}
#endif


// ==========================================
// 2. Helper Services (GPS & UUID)
// ==========================================

class TripIdGenerator {
public:
    static string generate() {
        static const int64_t start_us = duration_cast<microseconds>(
            system_clock::now().time_since_epoch()).count();
        static atomic<uint64_t> counter{0};
        uint64_t id = counter.fetch_add(1, memory_order_relaxed);
        return to_string(start_us) + "-" + to_string(id);
    }
};

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
        return {40.7580, -73.9855};
    }

    // Linear Interpolation
    pair<double, double> interpolate(int pu_id, int do_id, 
                                     int64_t start_ts, int64_t end_ts, int64_t current_ts) {
        auto start_pos = get_coords(pu_id);
        auto end_pos = get_coords(do_id);

        double total_duration = (double)(end_ts - start_ts);
        if (total_duration <= 0) return start_pos;

        double elapsed = (double)(current_ts - start_ts);
        double ratio = elapsed / total_duration;

        if (ratio >= 1.0) return end_pos;

        double lat = start_pos.first + (end_pos.first - start_pos.first) * ratio;
        double lon = start_pos.second + (end_pos.second - start_pos.second) * ratio;
        return {lat, lon};
    }
};

// ==========================================
// 3. Core Engine (Simulator)
// ==========================================

class UberSimulator {
private:
    priority_queue<SimulationEvent, vector<SimulationEvent>, greater<SimulationEvent>> event_queue;
    deque<RawTripData> dataset; // In-memory storage (stable addresses)
    GeoService geo_service;
    double playback_speed;
    string ingestion_url;
    HttpEndpoint ingestion_endpoint;
    bool ingestion_endpoint_ok = false;
    mutex log_mu;
    mutex ingest_mu;
    
    // Config
    const int64_t TRANSIT_UPDATE_INTERVAL_US = 30 * 1000000; // 30 seconds
    const size_t PAYLOAD_QUEUE_CAPACITY = 4096;

public:
    UberSimulator(double speed, string url) : playback_speed(speed), ingestion_url(std::move(url)) {
        // deque keeps element addresses stable when it grows.
        ingestion_endpoint_ok = parse_http_url(ingestion_url, ingestion_endpoint);
        if (!ingestion_endpoint_ok) {
            lock_guard<mutex> lock(log_mu);
            cerr << "[Config] Invalid ingestion_url; falling back to stdout." << endl;
        }
    }

    // Add data to engine and generate sub-events
    void ingest_trip(RawTripData data) {
        vector<RawTripData> batch;
        batch.push_back(std::move(data));
        ingest_trips(std::move(batch));
    }

    void ingest_trips(vector<RawTripData>&& batch) {
        if (batch.empty()) return;
        lock_guard<mutex> lock(ingest_mu);
        for (auto& data : batch) {
            data.trip_id = TripIdGenerator::generate();
            dataset.push_back(std::move(data));
            const RawTripData* p_data = &dataset.back();
            const auto& stored = *p_data;

            // 1. PICKUP
            event_queue.push({EventType::PICKUP, stored.tpep_pickup_datetime, p_data, 0.0, 0.0});

            // 2. IN_TRANSIT (Loop)
            int64_t current_time = stored.tpep_pickup_datetime + TRANSIT_UPDATE_INTERVAL_US;
            while (current_time < stored.tpep_dropoff_datetime) {
                auto coords = geo_service.interpolate(stored.PULocationID, stored.DOLocationID,
                                                      stored.tpep_pickup_datetime, stored.tpep_dropoff_datetime,
                                                      current_time);
                event_queue.push({EventType::IN_TRANSIT, current_time, p_data, coords.first, coords.second});
                current_time += TRANSIT_UPDATE_INTERVAL_US;
            }

            // 3. DROPOFF
            event_queue.push({EventType::DROPOFF, stored.tpep_dropoff_datetime, p_data, 0.0, 0.0});
        }
    }

    void run() {
        if (event_queue.empty()) {
            cout << "No events to simulate." << endl;
            return;
        }

        cout << ">>> Starting Simulation (Speed: " << playback_speed << "x) <<<" << endl;
        int64_t sim_start_ts = event_queue.top().event_time_us;
        cout << ">>> First Event Time (Unix us): " << sim_start_ts << endl;

        BoundedQueue payload_queue(PAYLOAD_QUEUE_CAPACITY);

        size_t sender_count = thread::hardware_concurrency();
        if (sender_count == 0) sender_count = 1;
        sender_count *= 2;

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

        thread scheduler(&UberSimulator::schedule_events, this, std::ref(payload_queue), sim_start_ts);
        scheduler.join();
        for (auto& t : senders) {
            t.join();
        }
        cout << ">>> Simulation Completed <<<" << endl;
    }

private:
    void schedule_events(BoundedQueue& payload_queue, int64_t sim_start_ts) {
        auto wall_start = steady_clock::now();

        while (!event_queue.empty()) {
            SimulationEvent ev = event_queue.top();

            // Calculate current simulation time
            auto now = steady_clock::now();
            double real_elapsed_sec = duration<double>(now - wall_start).count();
            int64_t sim_elapsed_us = (int64_t)(real_elapsed_sec * playback_speed * 1000000.0);
            int64_t current_sim_ts = sim_start_ts + sim_elapsed_us;

            if (current_sim_ts >= ev.event_time_us) {
                string payload = build_payload(ev);
                if (!payload_queue.push(std::move(payload))) {
                    break;
                }
                event_queue.pop();
            } else {
                // Sleep only if wait is significant (>1ms) to avoid CPU spin
                double wait_sec = (double)(ev.event_time_us - current_sim_ts) / 1000000.0 / playback_speed;
                if (wait_sec > 0.001) {
                    this_thread::sleep_for(duration<double>(wait_sec));
                }
            }
        }

        payload_queue.close();
    }

    string build_payload(const SimulationEvent& ev) {
        stringstream json;
        const auto* d = ev.raw_data;

        json << "{";
        if (ev.type == EventType::PICKUP) {
            json << "\"event\": \"PICKUP\", "
                 << "\"trip_id\": \"" << d->trip_id << "\", "
                 << "\"pu_loc\": " << d->PULocationID << ", "
                 << "\"vendor_id\": " << d->VendorID << ", "
                 << "\"passenger\": " << d->passenger_count << ", "
                 << "\"ts\": " << d->tpep_pickup_datetime;
        } 
        else if (ev.type == EventType::IN_TRANSIT) {
            json << "\"event\": \"IN_TRANSIT\", "
                 << "\"trip_id\": \"" << d->trip_id << "\", "
                 << "\"lat\": " << fixed << setprecision(6) << ev.current_lat << ", "
                 << "\"lon\": " << ev.current_lon << ", "
                 << "\"ts\": " << ev.event_time_us;
        } 
        else { // DROPOFF
            json << "\"event\": \"DROPOFF\", "
                 << "\"trip_id\": \"" << d->trip_id << "\", "
                 << "\"do_loc\": " << d->DOLocationID << ", "
                 << "\"fare\": " << d->fare_amount << ", "
                 << "\"total\": " << d->total_amount << ", "
                 << "\"dist\": " << d->trip_distance << ", "
                 << "\"ts\": " << d->tpep_dropoff_datetime;
        }
        json << "}";

        return json.str();
    }

    void post_payload(const string& payload) {
        if (!ingestion_endpoint_ok) {
            lock_guard<mutex> lock(log_mu);
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

        size_t worker_count = thread::hardware_concurrency();
        if (worker_count == 0) worker_count = 1;
        worker_count *= 2;

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
        int year = 0;
        int month = 0;
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

    static int64_t first_nonzero(const vector<int64_t>& values) {
        for (int64_t value : values) {
            if (value != 0) return value;
        }
        if (values.empty()) return 0;
        return values.front();
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
                                     const shared_ptr<arrow::ChunkedArray>& column,
                                     const vector<int64_t>& values) {
        if (values.empty()) return;
        static mutex log_mu;
        int64_t raw = first_nonzero(values);
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
            cout << " range=out_of_range";
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

        std::shared_ptr<arrow::Table> table;
        st = reader->ReadTable(&table);
        if (!st.ok()) {
            cerr << "[Loader] Failed to read table for " << path << ": " << st.ToString() << endl;
            return;
        }
        cout << "[Loader] Table rows: " << table->num_rows() << endl;

        // Column Extraction Helpers
        auto get_int64 = [&](string name) { return extract_int64(table, name); };
        auto get_double = [&](string name) { return extract_double(table, name); };
        auto get_string = [&](string name) { return extract_string(table, name); };

        auto pu_col = table->GetColumnByName("tpep_pickup_datetime");
        auto do_col = table->GetColumnByName("tpep_dropoff_datetime");

        auto pu_times = get_int64("tpep_pickup_datetime");
        auto do_times = get_int64("tpep_dropoff_datetime");
        auto pu_locs = get_int64("PULocationID");
        auto do_locs = get_int64("DOLocationID");
        auto vendors = get_int64("VendorID");
        auto ratecodes = get_int64("RatecodeID");
        auto payment_types = get_int64("payment_type");
        auto store_flags = get_string("store_and_fwd_flag");
        auto passengers = get_int64("passenger_count");

        auto congestion = get_double("congestion_surcharge");
        auto extras = get_double("extra");
        auto fares = get_double("fare_amount");
        auto improvements = get_double("improvement_surcharge");
        auto mta_taxes = get_double("mta_tax");
        auto tips = get_double("tip_amount");
        auto tolls = get_double("tolls_amount");
        auto totals = get_double("total_amount");
        auto dists = get_double("trip_distance");
        auto airport_fees = get_double("Airport_fee");

        log_timestamp_sample(path, "pickup", pu_col, pu_times);
        log_timestamp_sample(path, "dropoff", do_col, do_times);

        if (pu_times.empty() || do_times.empty()) {
            cerr << "[Loader] Missing required timestamps in: " << path << endl;
            return;
        }

        size_t row_count = min(pu_times.size(), do_times.size());
        size_t ingested = 0;
        size_t skipped_airport = 0;
        size_t skipped_time = 0;
        vector<RawTripData> batch;
        batch.reserve(row_count);

        // Row-wise Injection
        for (size_t i = 0; i < row_count; ++i) {
            RawTripData data;
            data.tpep_pickup_datetime = normalize_epoch_us(pu_times[i]);
            data.tpep_dropoff_datetime = normalize_epoch_us(do_times[i]);
            data.PULocationID = (i < pu_locs.size()) ? pu_locs[i] : 0;
            data.DOLocationID = (i < do_locs.size()) ? do_locs[i] : 0;
            if (data.PULocationID == 264 || data.PULocationID == 265 ||
                data.DOLocationID == 264 || data.DOLocationID == 265) {
                skipped_airport++;
                continue;
            }
            data.VendorID = (i < vendors.size()) ? vendors[i] : 0;
            data.RatecodeID = (i < ratecodes.size()) ? ratecodes[i] : 0;
            data.payment_type = (i < payment_types.size()) ? payment_types[i] : 0;
            data.store_and_fwd_flag = (i < store_flags.size()) ? store_flags[i] : "";
            data.passenger_count = (i < passengers.size()) ? passengers[i] : 0;

            data.congestion_surcharge = (i < congestion.size()) ? congestion[i] : 0.0;
            data.extra = (i < extras.size()) ? extras[i] : 0.0;
            data.fare_amount = (i < fares.size()) ? fares[i] : 0.0;
            data.improvement_surcharge = (i < improvements.size()) ? improvements[i] : 0.0;
            data.mta_tax = (i < mta_taxes.size()) ? mta_taxes[i] : 0.0;
            data.tip_amount = (i < tips.size()) ? tips[i] : 0.0;
            data.tolls_amount = (i < tolls.size()) ? tolls[i] : 0.0;
            data.total_amount = (i < totals.size()) ? totals[i] : 0.0;
            data.trip_distance = (i < dists.size()) ? dists[i] : 0.0;
            data.Airport_fee = (i < airport_fees.size()) ? airport_fees[i] : 0.0;

            if (data.tpep_dropoff_datetime < data.tpep_pickup_datetime) {
                skipped_time++;
                continue;
            }

            batch.push_back(std::move(data));
            ingested++;
        }
        sim.ingest_trips(std::move(batch));
        cout << "[Loader] " << path << " rows=" << row_count
             << " ingested=" << ingested
             << " skipped_airport=" << skipped_airport
             << " skipped_time=" << skipped_time << endl;
    }

    static vector<int64_t> extract_int64(shared_ptr<arrow::Table> table, string col_name) {
        auto col = table->GetColumnByName(col_name);
        vector<int64_t> result;
        if (!col) return result;
        result.reserve(table->num_rows());
        for (auto& chunk : col->chunks()) {
            for (int64_t i = 0; i < chunk->length(); ++i) {
                result.push_back(get_int64_value(chunk, i, 0));
            }
        }
        return result;
    }

    static vector<double> extract_double(shared_ptr<arrow::Table> table, string col_name) {
        auto col = table->GetColumnByName(col_name);
        vector<double> result;
        if (!col) return result;
        result.reserve(table->num_rows());
        for (auto& chunk : col->chunks()) {
            for (int64_t i = 0; i < chunk->length(); ++i) {
                result.push_back(get_double_value(chunk, i, 0.0));
            }
        }
        return result;
    }

    static vector<string> extract_string(shared_ptr<arrow::Table> table, string col_name) {
        auto col = table->GetColumnByName(col_name);
        vector<string> result;
        if (!col) return result;
        result.reserve(table->num_rows());
        for (auto& chunk : col->chunks()) {
            for (int64_t i = 0; i < chunk->length(); ++i) {
                result.push_back(get_string_value(chunk, i));
            }
        }
        return result;
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

    // Initialize Simulator
    UberSimulator sim(config.playback_speed, config.ingestion_url);

    // Load Data
    cout << "=== Uber Data Generator Initializing ===" << endl;
    ParquetLoader::load_from_directory(config.data_path, sim, config.range);

    // Run
    sim.run();

    return 0;
}

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

// Apache Arrow & Parquet Headers
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>

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

// ==========================================
// 2. Helper Services (GPS & UUID)
// ==========================================

class TripIdGenerator {
public:
    static string generate() {
        static random_device rd;
        static mt19937 gen(rd());
        static uniform_int_distribution<> dis(0, 15);
        static const char* digits = "0123456789abcdef";
        string uuid(36, ' ');
        for (int i = 0; i < 36; i++) {
            if (i == 8 || i == 13 || i == 18 || i == 23) uuid[i] = '-';
            else uuid[i] = digits[dis(gen)];
        }
        return uuid;
    }
};

class GeoService {
private:
    map<int, pair<double, double>> location_map;

public:
    GeoService() {
        // MOCK DATA: 실제 환경에서는 'taxi_zone_lookup.csv'를 로드해야 함
        location_map[142] = {40.7711, -73.9841}; // Lincoln Square East
        location_map[230] = {40.7629, -73.9860}; // Times Sq
        location_map[100] = {40.7505, -73.9934}; // Garment District
        location_map[161] = {40.7559, -73.9864}; // Midtown Center
        // Default fallback for unknown IDs
    }

    pair<double, double> get_coords(int location_id) {
        if (location_map.count(location_id)) return location_map[location_id];
        return {40.7580, -73.9855}; // Default: NYC Center
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
    vector<RawTripData> dataset; // In-memory storage
    GeoService geo_service;
    double playback_speed;
    
    // Config
    const int64_t TRANSIT_UPDATE_INTERVAL_US = 30 * 1000000; // 30 seconds

public:
    UberSimulator(double speed) : playback_speed(speed) {
        // Reserve memory to prevent reallocation invalidating pointers
        dataset.reserve(1000000); 
    }

    // Add data to engine and generate sub-events
    void ingest_trip(RawTripData data) {
        data.trip_id = TripIdGenerator::generate();
        
        // Store in vector first
        dataset.push_back(data);
        const RawTripData* p_data = &dataset.back();

        // 1. PICKUP
        event_queue.push({EventType::PICKUP, data.tpep_pickup_datetime, p_data, 0.0, 0.0});

        // 2. IN_TRANSIT (Loop)
        int64_t current_time = data.tpep_pickup_datetime + TRANSIT_UPDATE_INTERVAL_US;
        while (current_time < data.tpep_dropoff_datetime) {
            auto coords = geo_service.interpolate(data.PULocationID, data.DOLocationID,
                                                  data.tpep_pickup_datetime, data.tpep_dropoff_datetime,
                                                  current_time);
            event_queue.push({EventType::IN_TRANSIT, current_time, p_data, coords.first, coords.second});
            current_time += TRANSIT_UPDATE_INTERVAL_US;
        }

        // 3. DROPOFF
        event_queue.push({EventType::DROPOFF, data.tpep_dropoff_datetime, p_data, 0.0, 0.0});
    }

    void run() {
        if (event_queue.empty()) {
            cout << "No events to simulate." << endl;
            return;
        }

        // Time Synchronization
        int64_t sim_start_ts = event_queue.top().event_time_us;
        auto wall_start = steady_clock::now();

        cout << ">>> Starting Simulation (Speed: " << playback_speed << "x) <<<" << endl;
        cout << ">>> First Event Time (Unix us): " << sim_start_ts << endl;

        while (!event_queue.empty()) {
            const auto& ev = event_queue.top();

            // Calculate current simulation time
            auto now = steady_clock::now();
            double real_elapsed_sec = duration<double>(now - wall_start).count();
            int64_t sim_elapsed_us = (int64_t)(real_elapsed_sec * playback_speed * 1000000.0);
            int64_t current_sim_ts = sim_start_ts + sim_elapsed_us;

            if (current_sim_ts >= ev.event_time_us) {
                // Fire Event
                send_network_request(ev);
                event_queue.pop();
            } else {
                // Sleep Management
                double wait_sec = (double)(ev.event_time_us - current_sim_ts) / 1000000.0 / playback_speed;
                // Sleep only if wait is significant (>1ms) to avoid CPU spin
                if (wait_sec > 0.001) {
                    this_thread::sleep_for(duration<double>(wait_sec));
                }
            }
        }
        cout << ">>> Simulation Completed <<<" << endl;
    }

private:
    // Network Sender Mock-up (Prints JSON)
    void send_network_request(const SimulationEvent& ev) {
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

        // In real impl: CURL or Boost.Asio post here
        cout << "[NET] " << json.str() << endl;
    }
};

// ==========================================
// 4. Parquet Loader
// ==========================================

class ParquetLoader {
public:
    static void load_from_directory(const string& root_path, UberSimulator& sim) {
        if (!fs::exists(root_path)) {
            cerr << "Error: Directory not found -> " << root_path << endl;
            return;
        }

        for (const auto& entry : fs::recursive_directory_iterator(root_path)) {
            if (entry.is_regular_file() && entry.path().extension() == ".parquet") {
                cout << "[Loader] Reading: " << entry.path().filename() << "... ";
                load_file(entry.path().string(), sim);
                cout << "Done." << endl;
            }
        }
    }

private:
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
        if (!file_res.ok()) return;
        
        std::unique_ptr<parquet::arrow::FileReader> reader;
        st = parquet::arrow::OpenFile(*file_res, pool, &reader);
        if (!st.ok()) return;

        std::shared_ptr<arrow::Table> table;
        st = reader->ReadTable(&table);
        if (!st.ok()) return;

        // Column Extraction Helpers
        auto get_int64 = [&](string name) { return extract_int64(table, name); };
        auto get_double = [&](string name) { return extract_double(table, name); };
        auto get_string = [&](string name) { return extract_string(table, name); };

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

        if (pu_times.empty() || do_times.empty()) {
            cerr << "[Loader] Missing required timestamps in: " << path << endl;
            return;
        }

        size_t row_count = min(pu_times.size(), do_times.size());

        // Row-wise Injection
        for (size_t i = 0; i < row_count; ++i) {
            RawTripData data;
            data.tpep_pickup_datetime = pu_times[i];
            data.tpep_dropoff_datetime = do_times[i];
            data.PULocationID = (i < pu_locs.size()) ? pu_locs[i] : 0;
            data.DOLocationID = (i < do_locs.size()) ? do_locs[i] : 0;
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

            if (data.tpep_dropoff_datetime < data.tpep_pickup_datetime) continue;

            sim.ingest_trip(data);
        }
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
    // Configuration
    double playback_speed = 100.0; // 100x Speed
    string data_path = "../data/taxi_data_preprocessed";

    // Initialize Simulator
    UberSimulator sim(playback_speed);

    // Load Data
    cout << "=== Uber Data Generator Initializing ===" << endl;
    ParquetLoader::load_from_directory(data_path, sim);

    // Run
    sim.run();

    return 0;
}

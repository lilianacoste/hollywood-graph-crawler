#include <string>
#include "rapidjson-master/include/rapidjson/document.h"
#include <iostream>
#include <curl/curl.h>
#include <vector>
#include <string>
#include <chrono>
#include <unordered_set>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <algorithm> // for min

#include <rapidjson/error/en.h>


struct ParseException : std::runtime_error, rapidjson::ParseResult {
    ParseException(rapidjson::ParseErrorCode code, const char* msg, size_t offset) : 
        std::runtime_error(msg), 
        rapidjson::ParseResult(code, offset) {}
};

#define RAPIDJSON_PARSE_ERROR_NORETURN(code, offset) \
    throw ParseException(code, #code, offset)


using namespace std;
using namespace rapidjson;

bool debug = false;

// Updated service URL
const string SERVICE_URL = "http://hollywood-graph-crawler.bridgesuncc.org/neighbors/";

// Function to HTTP ecnode parts of URLs. for instance, replace spaces with '%20' for URLs
string url_encode(CURL* curl, string input) {
  char* out = curl_easy_escape(curl, input.c_str(), input.size()); //encode input string
  string s = out; //convert to string and return
  curl_free(out);
  return s; 
}

// Callback function for writing response data
size_t WriteCallback(void* contents, size_t size, size_t nmemb, string* output) {
    size_t totalSize = size * nmemb;
    output->append((char*)contents, totalSize);
    return totalSize;
}

// Function to fetch neighbors using libcurl with debugging
string fetch_neighbors(CURL* curl, const string& node) {
    if (!curl) {
        cerr << "Error: Invalid CURL handle" << endl;
        return "{}";
    }

    string url = SERVICE_URL + url_encode(curl, node);
    string response;

    if (debug)
        cout << "Sending request to: " << url << endl;

    // Set CURL options
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10L); // 10 second timeout
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 5L); // 5 second connection timeout
    
    // Add retry mechanism for transient errors
    curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);
    // curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L); // Verbose Logging (uncomment for debugging)

    // Set a User-Agent header to avoid potential blocking by the server
    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "User-Agent: C++-Client/1.0");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    // Perform the request with retry logic
    CURLcode res = CURLE_OK;
    int max_retries = 3;
    int retry_count = 0;
    
    do {
        response.clear();  // Clear the response buffer before each attempt
        res = curl_easy_perform(curl);
        
        if (res != CURLE_OK) {
            cerr << "CURL error on attempt " << (retry_count + 1) << ": " 
                 << curl_easy_strerror(res) << " for URL: " << url << endl;
            
            if (++retry_count >= max_retries) break;
            
            // Exponential backoff
            this_thread::sleep_for(chrono::milliseconds(500 * retry_count));
        }
    } while (res != CURLE_OK && retry_count < max_retries);

    if (res == CURLE_OK) {
        if (debug)
            cout << "CURL request successful!" << endl;
    } else {
        cerr << "Failed to fetch data after " << max_retries << " attempts for node: " << node << endl;
    }

    // Cleanup
    curl_slist_free_all(headers);

    if (debug) 
        cout << "Response received: " << response << endl;  // Debug log

    return (res == CURLE_OK) ? response : "{}";
}

// Function to parse JSON and extract neighbors
vector<string> get_neighbors(const string& json_str) {
    vector<string> neighbors;
    
    // Handle empty or invalid JSON
    if (json_str.empty() || json_str == "{}" || json_str == "[]") {
        return neighbors;
    }
    
    try {
        Document doc;
        ParseResult parseResult = doc.Parse(json_str.c_str());
        
        if (!parseResult) {
            cerr << "JSON parse error: " << GetParseError_En(parseResult.Code()) 
                 << " at offset " << parseResult.Offset() << endl;
            return neighbors;
        }
        
        if (doc.HasMember("neighbors") && doc["neighbors"].IsArray()) {
            const Value& neighborsArray = doc["neighbors"];
            for (SizeType i = 0; i < neighborsArray.Size(); i++) {
                if (neighborsArray[i].IsString()) {
                    neighbors.push_back(neighborsArray[i].GetString());
                }
            }
        } else {
            cerr << "JSON missing 'neighbors' array or invalid format" << endl;
        }
    } catch (const ParseException& e) {
        cerr << "Error while parsing JSON: " << e.what() << endl;
        if (debug) {
            cerr << "Problematic JSON: " << json_str << endl;
        }
    } catch (const std::exception& e) {
        cerr << "Unexpected error during JSON parsing: " << e.what() << endl;
    }
    
    return neighbors;
}

mutex mtx;
condition_variable cv;
const int MAX_THREADS = 8;
// BFS Traversal Function
vector<vector<string>> bfs(CURL* curl, const string& start, int depth) {
  vector<vector<string>> levels;
  unordered_set<string> visited;
  levels.push_back({start});
  visited.insert(start);

  for (int d = 0; d < depth; d++) {
      cout << "Starting level: " << d << endl;
      levels.push_back({});

      vector<thread> threads;
      int num_nodes = levels[d].size();
      
      // Calculate appropriate number of threads to use
      int num_threads = min(MAX_THREADS, num_nodes);
      
      // Calculate nodes per thread
      int nodes_per_thread = (num_threads > 0) ? (num_nodes / num_threads) : 0;
      int remainder = (num_threads > 0) ? (num_nodes % num_threads) : 0;
      
      cout << "Level " << d << " has " << num_nodes << " nodes, using " 
           << num_threads << " threads with " << nodes_per_thread 
           << " nodes per thread (plus " << remainder << " remainder)" << endl;

      // Distribute work to threads
      for (int i = 0; i < num_threads; i++) {
          int start_idx = i * nodes_per_thread;
          int end_idx = (i + 1) * nodes_per_thread;
          if (i == num_threads - 1) {
              end_idx += remainder;
          }

          if (start_idx < num_nodes) {
              threads.push_back(thread([=, &visited, &levels]() {
                  // Create a new CURL handle for each thread
                  CURL* thread_curl = curl_easy_init();
                  if (!thread_curl) {
                      cerr << "Failed to initialize CURL in thread" << endl;
                      return;
                  }
                  
                  for (int j = start_idx; j < end_idx && j < num_nodes; j++) {
                      string node = levels[d][j];
                      vector<string> neighbors = get_neighbors(fetch_neighbors(thread_curl, node));
                      for (const auto& neighbor : neighbors) {
                          lock_guard<mutex> lock(mtx);  // Mutex to prevent race conditions
                          if (!visited.count(neighbor)) {
                              visited.insert(neighbor);
                              levels[d + 1].push_back(neighbor);
                          }
                      }
                  }
                  
                  // Clean up the CURL handle
                  curl_easy_cleanup(thread_curl);
              }));
          }
      }

      // Join threads after work is done
      for (auto& t : threads) {
          t.join();
      }
  }

  return levels;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        cerr << "Usage: " << argv[0] << " <node_name> <depth>\n";
        return 1;
    }

    string start_node = argv[1];     // example "Tom%20Hanks"
    int depth;
    try {
        depth = stoi(argv[2]);
    } catch (const exception& e) {
        cerr << "Error: Depth must be an integer.\n";
        return 1;
    }

    CURL* curl = curl_easy_init();
    if (!curl) {
        cerr << "Failed to initialize CURL" << endl;
        return -1;
    }


    const auto start{std::chrono::steady_clock::now()};
    
    
    for (const auto& n : bfs(curl, start_node, depth)) {
      for (const auto& node : n)
        cout << "- " << node << "\n";
      std::cout<<n.size()<<"\n";
    }
    
    const auto finish{std::chrono::steady_clock::now()};
    const std::chrono::duration<double> elapsed_seconds{finish - start};
    std::cout << "Time to crawl: "<<elapsed_seconds.count() << "s\n";
    
    curl_easy_cleanup(curl);

    
    return 0;
}

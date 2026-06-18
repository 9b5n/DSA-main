// CacheGrid - Distributed Key-Value Database Cache Network
// Semester II DSA Project | Compile: g++ -std=c++17 -o cachegrid main.cpp

#include <iostream>
#include <string>
#include <unordered_map>
#include <stack>
#include <queue>
#include <set>
#include <vector>
#include <list>
#include <climits>
#include <algorithm>
#include <limits>
#include <cstdlib>  // For rand()
#include <ctime>    // For srand()
using namespace std;

// Utility function to clear input buffer
void clearInput() {
    cin.ignore(numeric_limits<streamsize>::max(), '\n');
}

// ============================================================
// 1. KeyIndexManager - Maintains consistent index of data keys
// Uses unordered_map: O(1) average insert/search
// ============================================================
class KeyIndexManager {
    unordered_map<string, string> keyIndex; // key -> server mapping
    
public:
    KeyIndexManager() {
        // Seed random for server selection
        srand(time(nullptr));
    }
    
    void addKey(const string& key, const string& server) {
        keyIndex[key] = server;
        cout << "[KeyIndex] \"" << key << "\" -> " << server << "\n";
    }
    
    void searchKey(const string& key) {
        auto it = keyIndex.find(key);
        if (it != keyIndex.end()) {
            cout << "[KeyIndex] \"" << key << "\" on Server: " << it->second << "\n";
        } else {
            cout << "[KeyIndex] \"" << key << "\" not found.\n";
        }
    }
    
    void removeKey(const string& key) {
        if (keyIndex.erase(key)) {
            cout << "[KeyIndex] Removed \"" << key << "\"\n";
        } else {
            cout << "[KeyIndex] \"" << key << "\" not found.\n";
        }
    }
    
    void displayAllKeys() {
        if (keyIndex.empty()) { 
            cout << "[KeyIndex] Empty.\n"; 
            return; 
        }
        cout << "[KeyIndex] All keys:\n";
        for (auto& e : keyIndex) {
            cout << "  " << e.first << " -> " << e.second << "\n";
        }
    }
    
    string getServer(const string& key) {
        auto it = keyIndex.find(key);
        return (it != keyIndex.end()) ? it->second : "";
    }
};

// ============================================================
// ============================================================
//  Forward declaration(s) (kept only for pointers)
//  NOTE: RequestManager needs the full DataStore definition to call methods,
//  so DataStore must be defined before RequestManager.
// ============================================================

// ============================================================
// 2. RollbackManager - Tracks changes for rollback
// Uses stack: O(1) push/pop
// ============================================================
class RollbackManager {
    struct WriteOperation {
        string operation;
        string key;
        string oldValue;
        string newValue;
        string server;
        long long timestamp;
    };
    
    stack<WriteOperation> history;
    long long operationCounter = 0;
    
public:
    void addWriteOperation(const string& key, const string& newVal, 
                          const string& oldVal = "", const string& server = "unknown") {
        WriteOperation op;
        op.operation = "WRITE";
        op.key = key;
        op.oldValue = oldVal;
        op.newValue = newVal;
        op.server = server;
        op.timestamp = ++operationCounter;
        
        history.push(op);
        cout << "[Rollback] Recorded: \"" << key << "\" = \"" << newVal 
             << "\" (on " << server << ")\n";
    }
    
    void addDeleteOperation(const string& key, const string& oldVal = "", 
                           const string& server = "unknown") {
        WriteOperation op;
        op.operation = "DELETE";
        op.key = key;
        op.oldValue = oldVal;
        op.newValue = "";
        op.server = server;
        op.timestamp = ++operationCounter;
        
        history.push(op);
        cout << "[Rollback] Recorded DELETE: \"" << key << "\" (on " << server << ")\n";
    }
    
    void rollbackLastWrite() {
        if (history.empty()) { 
            cout << "[Rollback] Nothing to rollback.\n"; 
            return; 
        }
        
        WriteOperation op = history.top();
        history.pop();
        cout << "[Rollback] Undone: \"" << op.key << "\" ";
        
        if (op.operation == "WRITE") {
            if (op.oldValue.empty()) {
                cout << "(was new insert, now removed)\n";
            } else {
                cout << "(restored to \"" << op.oldValue << "\")\n";
            }
        } else if (op.operation == "DELETE") {
            cout << "(restored to \"" << op.oldValue << "\")\n";
        }
    }
    
    void showWriteHistory() {
        if (history.empty()) { 
            cout << "[Rollback] History empty.\n"; 
            return; 
        }
        
        stack<WriteOperation> temp = history;
        cout << "[Rollback] Write history (most recent first):\n";
        while (!temp.empty()) {
            WriteOperation op = temp.top();
            temp.pop();
            cout << "  #" << op.timestamp << " [" << op.operation << "] " 
                 << op.key << " -> \"" << op.newValue << "\"";
            if (!op.oldValue.empty()) {
                cout << " (was: \"" << op.oldValue << "\")";
            }
            cout << " @ " << op.server << "\n";
        }
    }
    
    bool hasHistory() const {
        return !history.empty();
    }
};

// ============================================================
// 3. DataStore - Actual data storage
// ============================================================
class DataStore {
    unordered_map<string, string> data;
    unordered_map<string, string> metadata; // Additional metadata per key

public:
    void setValue(const string& key, const string& value, const string& meta = "") {
        data[key] = value;
        if (!meta.empty()) metadata[key] = meta;
        cout << "[DataStore] Stored \"" << key << "\" = \"" << value << "\"\n";
    }

    string getValue(const string& key) {
        if (data.count(key)) {
            return data[key];
        }
        return "";
    }

    void deleteValue(const string& key) {
        if (data.erase(key)) {
            metadata.erase(key);
            cout << "[DataStore] Deleted \"" << key << "\"\n";
        } else {
            cout << "[DataStore] \"" << key << "\" not found.\n";
        }
    }

    bool exists(const string& key) const {
        return data.count(key) > 0;
    }

    void displayAll() {
        if (data.empty()) {
            cout << "[DataStore] Empty.\n";
            return;
        }
        cout << "[DataStore] Stored data:\n";
        for (auto& pair : data) {
            cout << "  \"" << pair.first << "\" = \"" << pair.second << "\"";
            if (metadata.count(pair.first)) {
                cout << " [meta: " << metadata[pair.first] << "]";
            }
            cout << "\n";
        }
    }
};

// ============================================================
// 4. RequestManager - Handles stream of requests in order
// Uses queue: O(1) enqueue/dequeue
// ============================================================
class RequestManager {
    struct Request {
        string type;      // "READ", "WRITE", "DELETE"
        string key;
        string value;
        string client;
        long long timestamp;
    };
    
    queue<Request> requestQueue;
    long long requestCounter = 0;
    
public:
    void addReadRequest(const string& key, const string& client = "unknown") {
        Request req;
        req.type = "READ";
        req.key = key;
        req.client = client;
        req.timestamp = ++requestCounter;
        requestQueue.push(req);
        cout << "[Request] Queued READ: \"" << key << "\" from " << client << "\n";
    }
    
    void addWriteRequest(const string& key, const string& value, 
                        const string& client = "unknown") {
        Request req;
        req.type = "WRITE";
        req.key = key;
        req.value = value;
        req.client = client;
        req.timestamp = ++requestCounter;
        requestQueue.push(req);
        cout << "[Request] Queued WRITE: \"" << key << "\" = \"" << value 
             << "\" from " << client << "\n";
    }
    
    void addDeleteRequest(const string& key, const string& client = "unknown") {
        Request req;
        req.type = "DELETE";
        req.key = key;
        req.client = client;
        req.timestamp = ++requestCounter;
        requestQueue.push(req);
        cout << "[Request] Queued DELETE: \"" << key << "\" from " << client << "\n";
    }
    
    void processRequest(KeyIndexManager* keyIndex = nullptr, 
                       RollbackManager* rollback = nullptr,
                       DataStore* dataStore = nullptr) {
        if (requestQueue.empty()) { 
            cout << "[Request] Queue empty.\n"; 
            return; 
        }
        
        Request req = requestQueue.front();
        requestQueue.pop();
        
        cout << "[Request] Processing #" << req.timestamp << " [" << req.type 
             << "] \"" << req.key << "\"";
        if (!req.value.empty()) {
            cout << " = \"" << req.value << "\"";
        }
        cout << " from " << req.client << "\n";
        
        // Actually process the request if managers are provided
        if (req.type == "WRITE") {
            if (dataStore) {
                string oldValue = dataStore->getValue(req.key);
                dataStore->setValue(req.key, req.value);
                if (keyIndex) {
                    string server = keyIndex->getServer(req.key);
                    if (server.empty()) {
                        server = "server_" + to_string(req.timestamp % 5 + 1);
                        keyIndex->addKey(req.key, server);
                    }
                }
                if (rollback) {
                    rollback->addWriteOperation(req.key, req.value, oldValue);
                }
            }
        } else if (req.type == "DELETE") {
            if (dataStore) {
                string oldValue = dataStore->getValue(req.key);
                if (!oldValue.empty()) {
                    dataStore->deleteValue(req.key);
                    if (rollback) {
                        rollback->addDeleteOperation(req.key, oldValue);
                    }
                    if (keyIndex) {
                        keyIndex->removeKey(req.key);
                    }
                } else {
                    cout << "[Request] Key \"" << req.key << "\" not found.\n";
                }
            }
        } else if (req.type == "READ") {
            if (dataStore) {
                string value = dataStore->getValue(req.key);
                if (!value.empty()) {
                    cout << "[Request] READ result: \"" << req.key << "\" = \"" << value << "\"\n";
                } else {
                    cout << "[Request] READ: \"" << req.key << "\" not found.\n";
                }
            }
            if (keyIndex) {
                keyIndex->searchKey(req.key);
            }
        }
    }
    
    void processAllRequests(KeyIndexManager* keyIndex = nullptr, 
                           RollbackManager* rollback = nullptr,
                           DataStore* dataStore = nullptr) {
        int count = 0;
        while (!requestQueue.empty()) {
            processRequest(keyIndex, rollback, dataStore);
            count++;
        }
        cout << "[Request] Processed " << count << " requests total.\n";
    }
    
    void showPendingRequests() {
        if (requestQueue.empty()) { 
            cout << "[Request] No pending requests.\n"; 
            return; 
        }
        
        queue<Request> temp = requestQueue;
        int i = 1;
        cout << "[Request] Pending requests:\n";
        while (!temp.empty()) {
            Request req = temp.front();
            temp.pop();
            cout << "  " << i++ << ". #" << req.timestamp << " [" << req.type 
                 << "] \"" << req.key << "\"";
            if (!req.value.empty()) {
                cout << " = \"" << req.value << "\"";
            }
            cout << " (from " << req.client << ")\n";
        }
    }
    
    bool hasPendingRequests() const {
        return !requestQueue.empty();
    }
};

// ============================================================
// 4. ServerManager - Tracks active servers
// Uses set: O(log n) insert/find
// ============================================================
class ServerManager {
    set<string> activeServers;
    unordered_map<string, bool> serverHealth; // For health monitoring
    
public:
    void addServer(const string& server) {
        activeServers.insert(server);
        serverHealth[server] = true;
        cout << "[Server] Added: " << server << " (ACTIVE)\n";
    }
    
    void removeServer(const string& server) {
        if (activeServers.erase(server)) {
            serverHealth[server] = false;
            cout << "[Server] Removed: " << server << "\n";
        } else {
            cout << "[Server] " << server << " not found.\n";
        }
    }
    
    void checkServer(const string& server) {
        if (activeServers.count(server)) {
            bool healthy = serverHealth.count(server) ? serverHealth[server] : false;
            cout << "[Server] " << server << " is ACTIVE" 
                 << (healthy ? " and HEALTHY" : " (unhealthy)") << "\n";
        } else {
            cout << "[Server] " << server << " is INACTIVE\n";
        }
    }
    
    void showServers() {
        if (activeServers.empty()) { 
            cout << "[Server] None active.\n"; 
            return; 
        }
        cout << "[Server] Active servers:\n";
        for (auto& s : activeServers) {
            bool healthy = serverHealth.count(s) ? serverHealth[s] : false;
            cout << "  " << s << " (" << (healthy ? "HEALTHY" : "UNHEALTHY") << ")\n";
        }
    }
    
    bool isActive(const string& server) const {
        return activeServers.count(server) > 0;
    }
    
    string getRandomServer() const {
        if (activeServers.empty()) return "";
        auto it = activeServers.begin();
        advance(it, rand() % activeServers.size());
        return *it;
    }
};

// ============================================================
// 5. CompressionManager - Ranks data by compression ratio
// Uses priority_queue: O(log n) for insert/extract
// ============================================================
class CompressionManager {
    struct CompressionBlock {
        string name;
        int ratio;
        int size;
        string server;
        
        bool operator<(const CompressionBlock& other) const {
            return ratio < other.ratio; // Higher ratio = higher priority
        }
    };
    
    priority_queue<CompressionBlock> heap;
    unordered_map<string, CompressionBlock> blockMap;
    
public:
    void addMemoryBlock(const string& name, int ratio, int size = 1024, 
                       const string& server = "unknown") {
        CompressionBlock block{name, ratio, size, server};
        heap.push(block);
        blockMap[name] = block;
        cout << "[Compress] \"" << name << "\" ratio=" << ratio 
             << " size=" << size << "KB @ " << server << "\n";
    }
    
    void showCompressionPriority() {
        if (heap.empty()) { 
            cout << "[Compress] No blocks.\n"; 
            return; 
        }
        
        priority_queue<CompressionBlock> temp = heap;
        int rank = 1;
        cout << "[Compress] Compression priority (highest first):\n";
        while (!temp.empty()) {
            CompressionBlock block = temp.top();
            temp.pop();
            cout << "  " << rank++ << ". " << block.name 
                 << " (ratio=" << block.ratio 
                 << ", size=" << block.size << "KB"
                 << ", @ " << block.server << ")\n";
        }
    }
    
    void recommendCompression() {
        if (heap.empty()) {
            cout << "[Compress] No blocks to compress.\n";
            return;
        }
        
        CompressionBlock top = heap.top();
        cout << "[Compress] Best compression candidate: \"" << top.name 
             << "\" (ratio=" << top.ratio << ")\n";
        cout << "  Estimated savings: " << (top.size * top.ratio / 100) << "KB\n";
    }
};

// ============================================================
// 6. ServerGraph - Models network connectivity
// Uses adjacency list: O(V+E) space
// ============================================================
class ServerGraph {
public:
    struct Edge {
        string dest;
        int latency;
    };
    
    unordered_map<string, vector<Edge> > adj;
    unordered_map<string, unordered_map<string, int> > latencyMatrix;
    
    void addConnection(const string& a, const string& b, int latency) {
        if (latency < 0) {
            cout << "[Graph] Error: Latency cannot be negative.\n";
            return;
        }
        
        adj[a].push_back({b, latency});
        adj[b].push_back({a, latency});
        latencyMatrix[a][b] = latency;
        latencyMatrix[b][a] = latency;
        cout << "[Graph] " << a << " <-> " << b << " latency=" << latency << "ms\n";
    }
    
    void removeConnection(const string& a, const string& b) {
        // Remove from adj[a]
        vector<Edge>& vec = adj[a];
        vec.erase(remove_if(vec.begin(), vec.end(),
            [&](const Edge& e) { return e.dest == b; }), vec.end());
        
        // Remove from adj[b]
        vector<Edge>& vec2 = adj[b];
        vec2.erase(remove_if(vec2.begin(), vec2.end(),
            [&](const Edge& e) { return e.dest == a; }), vec2.end());
        
        // Remove from latency matrix
        latencyMatrix[a].erase(b);
        latencyMatrix[b].erase(a);
        
        cout << "[Graph] Removed connection: " << a << " <-> " << b << "\n";
    }
    
    void displayNetwork() {
        if (adj.empty()) { 
            cout << "[Graph] Empty.\n"; 
            return; 
        }
        
        cout << "[Graph] Network topology:\n";
        for (auto& node : adj) {
            cout << "  " << node.first << ":";
            if (node.second.empty()) {
                cout << " (isolated)";
            } else {
                for (auto& edge : node.second) {
                    cout << " -> " << edge.dest << "(" << edge.latency << "ms)";
                }
            }
            cout << "\n";
        }
    }
    
    void displayLatencyMatrix() {
        if (latencyMatrix.empty()) {
            cout << "[Graph] No connections.\n";
            return;
        }
        
        cout << "[Graph] Latency matrix (ms):\n";
        // Collect all nodes
        set<string> nodes;
        for (auto& pair : latencyMatrix) {
            nodes.insert(pair.first);
            for (auto& inner : pair.second) {
                nodes.insert(inner.first);
            }
        }
        
        // Print header
        cout << "     ";
        for (const string& node : nodes) {
            cout << node << " ";
        }
        cout << "\n";
        
        // Print matrix
        for (const string& src : nodes) {
            cout << src << " ";
            for (const string& dst : nodes) {
                if (src == dst) {
                    cout << "0   ";
                } else if (latencyMatrix.count(src) && latencyMatrix[src].count(dst)) {
                    cout << latencyMatrix[src][dst] << "   ";
                } else {
                    cout << "∞   ";
                }
            }
            cout << "\n";
        }
    }
    
    vector<string> getNodes() const {
        vector<string> nodes;
        for (auto& pair : adj) {
            nodes.push_back(pair.first);
        }
        return nodes;
    }
    
    bool hasNode(const string& node) const {
        return adj.count(node) > 0;
    }
};

// ============================================================
// 7. Dijkstra's Shortest Path Algorithm
// Uses priority_queue: O((V+E) log V)
// ============================================================
void shortestPath(ServerGraph& graph, const string& src, const string& dst) {
    if (!graph.hasNode(src) || !graph.hasNode(dst)) {
        cout << "[Dijkstra] Server not found.\n"; 
        return;
    }
    
    if (src == dst) {
        cout << "[Dijkstra] Source and destination are the same server.\n";
        cout << "[Dijkstra] Path: " << src << "\n";
        cout << "[Dijkstra] Total latency: 0 ms\n";
        return;
    }
    
    // Initialize distances and previous nodes
    unordered_map<string, int> dist;
    unordered_map<string, string> prev;
    
    // Get all nodes from graph
    vector<string> nodes = graph.getNodes();
    for (size_t i = 0; i < nodes.size(); ++i) {
        const string& node = nodes[i];
        dist[node] = INT_MAX;
        prev[node] = "";
    }
    
    dist[src] = 0;
    
    // Priority queue for Dijkstra
    priority_queue<pair<int, string>, vector<pair<int, string> >, greater<pair<int, string> > > pq;
    pq.push({0, src});
    
    while (!pq.empty()) {
        // Fix: Remove structured binding (C++17 feature)
        pair<int, string> top = pq.top();
        pq.pop();
        int currentDist = top.first;
        string current = top.second;
        
        // Skip if we found a better path already
        if (currentDist > dist[current]) continue;
        
        // Explore neighbors
        for (size_t i = 0; i < graph.adj[current].size(); ++i) {
            const ServerGraph::Edge& edge = graph.adj[current][i];
            int newDist = currentDist + edge.latency;
            
            if (newDist < dist[edge.dest]) {
                dist[edge.dest] = newDist;
                prev[edge.dest] = current;
                pq.push({newDist, edge.dest});
            }
        }
    }
    
    // Check if destination is reachable
    if (dist[dst] == INT_MAX) {
        cout << "[Dijkstra] No path found between " << src << " and " << dst << ".\n";
        return;
    }
    
    // Reconstruct path
    vector<string> path;
    string current = dst;
    int maxIterations = (int)nodes.size() + 1; // Safety check
    int iterations = 0;
    
    while (!current.empty() && iterations < maxIterations) {
        path.push_back(current);
        if (current == src) break;
        current = prev[current];
        iterations++;
    }
    
    // Reverse to get path from source to destination
    reverse(path.begin(), path.end());
    
    // Verify path starts with source
    if (path.empty() || path[0] != src) {
        cout << "[Dijkstra] Error: Path reconstruction failed.\n";
        return;
    }
    
    // Display path
    cout << "[Dijkstra] Shortest path from " << src << " to " << dst << ":\n";
    cout << "  ";
    for (size_t i = 0; i < path.size(); i++) {
        cout << path[i];
        if (i < path.size() - 1) {
            // Show the latency for this edge
            int edgeLatency = graph.latencyMatrix[path[i]][path[i+1]];
            cout << " -[" << edgeLatency << "ms]-> ";
        }
    }
    cout << "\n";
    cout << "[Dijkstra] Total latency: " << dist[dst] << " ms\n";
}

// ============================================================
// 8. LRUCache - Least Recently Used Cache
// Uses doubly linked list + unordered_map: O(1) get/put
// ============================================================
class LRUCache {
    int capacity;
    list<pair<string, string> > cache;  // (key, value) ordered by recency
    unordered_map<string, list<pair<string, string> >::iterator> keyMap;
    unordered_map<string, int> accessCount; // For statistics
    
public:
    LRUCache(int cap) : capacity(cap) {
        if (cap <= 0) capacity = 1; // Safety
    }
    
    void putData(const string& key, const string& value) {
        // If key exists, update it and move to front
        if (keyMap.count(key)) {
            keyMap[key]->second = value;
            cache.splice(cache.begin(), cache, keyMap[key]);
            cout << "[LRU] Updated \"" << key << "\" = \"" << value << "\"\n";
            return;
        }
        
        // If cache is full, evict LRU
        if ((int)cache.size() == capacity) {
            string evictedKey = cache.back().first;
            string evictedValue = cache.back().second;
            keyMap.erase(evictedKey);
            cache.pop_back();
            cout << "[LRU] Evicted LRU entry: \"" << evictedKey 
                 << "\" = \"" << evictedValue << "\"\n";
        }
        
        // Insert new entry at front
        cache.push_front({key, value});
        keyMap[key] = cache.begin();
        accessCount[key] = 0;
        cout << "[LRU] Inserted \"" << key << "\" = \"" << value << "\"\n";
    }
    
    string getData(const string& key) {
        if (!keyMap.count(key)) {
            cout << "[LRU] \"" << key << "\" not found.\n";
            return "";
        }
        
        // Move accessed item to front (most recently used)
        cache.splice(cache.begin(), cache, keyMap[key]);
        accessCount[key]++;
        cout << "[LRU] Accessed \"" << key << "\" = \"" << keyMap[key]->second 
             << "\" (access #" << accessCount[key] << ")\n";
        return keyMap[key]->second;
    }
    
    bool contains(const string& key) const {
        return keyMap.count(key) > 0;
    }
    
    void displayCache() {
        if (cache.empty()) { 
            cout << "[LRU] Cache empty.\n"; 
            return; 
        }
        
        cout << "[LRU] Cache contents (MRU -> LRU):\n";
        int i = 1;
        for (auto& entry : cache) {
            cout << "  " << i++ << ". \"" << entry.first << "\" = \"" 
                 << entry.second << "\"";
            if (accessCount.count(entry.first)) {
                cout << " (accessed " << accessCount[entry.first] << " times)";
            }
            cout << "\n";
        }
        cout << "  Cache size: " << cache.size() << "/" << capacity << "\n";
    }
    
    void clearCache() {
        cache.clear();
        keyMap.clear();
        accessCount.clear();
        cout << "[LRU] Cache cleared.\n";
    }
    
    void evictLRU() {
        if (cache.empty()) {
            cout << "[LRU] Cache already empty.\n";
            return;
        }
        string key = cache.back().first;
        keyMap.erase(key);
        accessCount.erase(key);
        cache.pop_back();
        cout << "[LRU] Evicted LRU: \"" << key << "\"\n";
    }
};

// ============================================================
// Main Program
// ============================================================
int main() {
    // Initialize all components
    KeyIndexManager keyIndex;
    RollbackManager rollback;
    RequestManager requestManager;
    ServerManager serverManager;
    CompressionManager compressionManager;
    ServerGraph serverGraph;
    DataStore dataStore;
    LRUCache lruCache(5);
    
    int choice;
    string input1, input2, input3;
    int intValue;
    
    cout << "\n╔══════════════════════════════════════════════════╗\n";
    cout << "║          CACHEGRID DISTRIBUTED SYSTEM          ║\n";
    cout << "║      Distributed Key-Value Database Cache      ║\n";
    cout << "╚══════════════════════════════════════════════════╝\n";
    
    do {
        cout << "\n┌───────────────── Menu ─────────────────┐\n";
        cout << "│  1. Add Data Key                     │\n";
        cout << "│  2. Search Data Key                  │\n";
        cout << "│  3. Remove Data Key                  │\n";
        cout << "│  4. Add Write Operation              │\n";
        cout << "│  5. Rollback Last Write              │\n";
        cout << "│  6. Show Write History               │\n";
        cout << "│  7. Add Read Request                 │\n";
        cout << "│  8. Add Write Request                │\n";
        cout << "│  9. Process Next Request             │\n";
        cout << "│ 10. Process All Requests             │\n";
        cout << "│ 11. Show Pending Requests            │\n";
        cout << "│ 12. Add Active Server                │\n";
        cout << "│ 13. Check Active Server              │\n";
        cout << "│ 14. Show All Servers                 │\n";
        cout << "│ 15. Add Compression Block            │\n";
        cout << "│ 16. Show Compression Priority        │\n";
        cout << "│ 17. Add Server Connection            │\n";
        cout << "│ 18. Display Network Topology         │\n";
        cout << "│ 19. Display Latency Matrix           │\n";
        cout << "│ 20. Find Shortest Path               │\n";
        cout << "│ 21. Access LRU Cache                 │\n";
        cout << "│ 22. Insert LRU Cache                 │\n";
        cout << "│ 23. Display LRU Cache                │\n";
        cout << "│ 24. Store Data in DataStore          │\n";
        cout << "│ 25. Retrieve Data from DataStore     │\n";
        cout << "│ 26. Display All DataStore            │\n";
        cout << "│ 27. Display All Keys Index           │\n";
        cout << "│  0. Exit                             │\n";
        cout << "└────────────────────────────────────────┘\n";
        cout << "Choice: ";
        
        cin >> choice;
        clearInput();
        
        switch(choice) {
            case 1:
                cout << "Enter key: ";
                getline(cin, input1);
                cout << "Enter server name: ";
                getline(cin, input2);
                keyIndex.addKey(input1, input2);
                break;
                
            case 2:
                cout << "Enter key to search: ";
                getline(cin, input1);
                keyIndex.searchKey(input1);
                break;
                
            case 3:
                cout << "Enter key to remove: ";
                getline(cin, input1);
                keyIndex.removeKey(input1);
                break;
                
            case 4:
                cout << "Enter key: ";
                getline(cin, input1);
                cout << "Enter value: ";
                getline(cin, input2);
                cout << "Enter server (optional): ";
                getline(cin, input3);
                if (input3.empty()) input3 = "unknown";
                
                // Store data
                dataStore.setValue(input1, input2);
                keyIndex.addKey(input1, input3);
                rollback.addWriteOperation(input1, input2, "", input3);
                break;
                
            case 5:
                rollback.rollbackLastWrite();
                break;
                
            case 6:
                rollback.showWriteHistory();
                break;
                
            case 7:
                cout << "Enter key to read: ";
                getline(cin, input1);
                cout << "Enter client name (optional): ";
                getline(cin, input2);
                if (input2.empty()) input2 = "unknown";
                requestManager.addReadRequest(input1, input2);
                break;
                
            case 8:
                cout << "Enter key: ";
                getline(cin, input1);
                cout << "Enter value: ";
                getline(cin, input2);
                cout << "Enter client name (optional): ";
                getline(cin, input3);
                if (input3.empty()) input3 = "unknown";
                requestManager.addWriteRequest(input1, input2, input3);
                break;
                
            case 9:
                requestManager.processRequest(&keyIndex, &rollback, &dataStore);
                break;
                
            case 10:
                requestManager.processAllRequests(&keyIndex, &rollback, &dataStore);
                break;
                
            case 11:
                requestManager.showPendingRequests();
                break;
                
            case 12:
                cout << "Enter server name: ";
                getline(cin, input1);
                serverManager.addServer(input1);
                break;
                
            case 13:
                cout << "Enter server name: ";
                getline(cin, input1);
                serverManager.checkServer(input1);
                break;
                
            case 14:
                serverManager.showServers();
                break;
                
            case 15:
                cout << "Enter block name: ";
                getline(cin, input1);
                cout << "Enter compression ratio (0-100): ";
                cin >> intValue;
                clearInput();
                cout << "Enter server (optional): ";
                getline(cin, input2);
                if (input2.empty()) input2 = "unknown";
                compressionManager.addMemoryBlock(input1, intValue, 1024, input2);
                break;
                
            case 16:
                compressionManager.showCompressionPriority();
                compressionManager.recommendCompression();
                break;
                
            case 17:
                cout << "Enter source server: ";
                getline(cin, input1);
                cout << "Enter destination server: ";
                getline(cin, input2);
                cout << "Enter latency (ms): ";
                cin >> intValue;
                clearInput();
                serverGraph.addConnection(input1, input2, intValue);
                break;
                
            case 18:
                serverGraph.displayNetwork();
                break;
                
            case 19:
                serverGraph.displayLatencyMatrix();
                break;
                
            case 20:
                cout << "Enter source server: ";
                getline(cin, input1);
                cout << "Enter destination server: ";
                getline(cin, input2);
                shortestPath(serverGraph, input1, input2);
                break;
                
            case 21:
                cout << "Enter key to access: ";
                getline(cin, input1);
                lruCache.getData(input1);
                lruCache.displayCache();
                break;
                
            case 22:
                cout << "Enter key: ";
                getline(cin, input1);
                cout << "Enter value: ";
                getline(cin, input2);
                lruCache.putData(input1, input2);
                lruCache.displayCache();
                break;
                
            case 23:
                lruCache.displayCache();
                break;
                
            case 24:
                cout << "Enter key: ";
                getline(cin, input1);
                cout << "Enter value: ";
                getline(cin, input2);
                dataStore.setValue(input1, input2);
                break;
                
            case 25:
                cout << "Enter key: ";
                getline(cin, input1);
                input2 = dataStore.getValue(input1);
                if (!input2.empty()) {
                    cout << "[DataStore] Retrieved: \"" << input1 << "\" = \"" << input2 << "\"\n";
                } else {
                    cout << "[DataStore] \"" << input1 << "\" not found.\n";
                }
                break;
                
            case 26:
                dataStore.displayAll();
                break;
                
            case 27:
                keyIndex.displayAllKeys();
                break;
                
            case 0:
                cout << "\n╔══════════════════════════════════════════════════╗\n";
                cout << "║           Thank you for using CacheGrid!        ║\n";
                cout << "╚══════════════════════════════════════════════════╝\n";
                break;
                
            default:
                cout << "Invalid choice. Please try again.\n";
                break;
        }
        
    } while (choice != 0);
    
    return 0;
}

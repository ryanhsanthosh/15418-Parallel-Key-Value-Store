
#include <iostream>
#include <map>
#include <cassert>
#include <thread>
#include <mutex>
#include <vector>
#include <unordered_map>
#include<string.h>

#define CAPACITY 500
#define MAXLEN 120

class LinkedListNode {
    public:
        int key;
        char *value;
        LinkedListNode* next;
        LinkedListNode* prev;
        int valid;
        std::mutex mux;
        LinkedListNode(int keyIns, std::string val){
            key = keyIns;
            value = new char[MAXLEN+1]();
            strcpy(value, val.c_str());
            next = nullptr;
            prev = nullptr;
            valid = 0;
        }
};

class CoarseGrainedKeyValueStore {
    private:
    LinkedListNode** map;
    std::mutex mux;
    public:
        CoarseGrainedKeyValueStore() {
            map = new LinkedListNode*[CAPACITY]();
            for (int i = 0; i < CAPACITY; i++){
                auto node = new LinkedListNode(-1, "");
                map[i] = node;
            }
        }
        // Insertion function
        void insert(int key, const std::string& value) {
            auto node = new LinkedListNode(key, value);
            node->valid = 1;
            mux.lock();
            auto it = map[key%CAPACITY];
            while(it->next){
                it = it->next;
            }
            node->prev = it;
            it->next = node;
            mux.unlock();
        }

        void update(int key, const std::string& value){
            mux.lock();
            auto it = map[key%CAPACITY];
            char* new_val = new char[MAXLEN+1]();
            strcpy(new_val, value.c_str());
            while (it){
                if (it->key == key && it->valid){
                    auto old_val = it->value;
                    it->value = new_val;
                    delete[] old_val;
                    mux.unlock();
                    return;
                    
                } else {
                    it = it->next;
                }
            }
            mux.unlock();
        }

        // Deletion function
        void remove(int key) {
            mux.lock();
            auto it = map[key%CAPACITY];
            while (it){
                if (it->key == key && it->valid){
                    if (it->prev){
                        it->prev->next = it->next;
                    }
                    if(it->next){
                        it->next->prev = it->prev;
                    }
                    mux.unlock();
                    return;
                } else {
                    it = it->next;
                }
            }
            mux.unlock();
        }

        // Lookup function
        void lookup(int key, std::string& val) {
            mux.lock();
            auto it = map[key%CAPACITY];
            while (it){
                if (it->key == key && it->valid){
                    val = it->value;
                    mux.unlock();
                    return;
                } else {
                    it = it->next;
                }
            }
            mux.unlock();
        }
};

class FineGrainedKeyValueStore {
    private:
    LinkedListNode** map;
    public:
        FineGrainedKeyValueStore() {
            map = new LinkedListNode*[CAPACITY]();
            for (int i = 0; i < CAPACITY; i++){
                auto node = new LinkedListNode(-1, "");
                map[i] = node;
            }
        }
        // Insertion function
        void insert(int key, const std::string& value) {
            auto node = new LinkedListNode(key, value);
            node->valid = 1;
            auto it = map[key%CAPACITY];
            it->mux.lock();
            while(it->next){
                it->mux.unlock();
                it = it->next;
                it->mux.lock();
            }
            node->prev = it;
            it->next = node;
            it->mux.unlock();
        }

        void update(int key, const std::string& value){
            auto it = map[key%CAPACITY];
            char* new_val = new char[MAXLEN+1]();
            strcpy(new_val, value.c_str());
            while (it){
                it->mux.lock();
                if (it->key == key && it->valid){
                    auto old_val = it->value;
                    it->value = new_val;
                    delete[] old_val;
                    it->mux.unlock();
                    return;
                    
                } else {
                    it->mux.unlock();
                    it = it->next;
                }
            }
        }

        // Deletion function
        void remove(int key) {
            auto it = map[key%CAPACITY];
            while (it){
                if (it->prev) {
                    it->prev->mux.lock();
                }
                it->mux.lock();
                if (it->next) {
                    it->next->mux.lock();
                }
                if (it->key == key && it->valid){
                    if(it->prev){
                        it->prev->next = it->next;
                        it->prev->mux.unlock();
                    }
                    if (it->next) {
                        it->next->prev = it->prev;
                        it->next->mux.unlock();
                    }
                    it->mux.unlock();
                    return;
                } else {
                    if(it->prev) {
                        it->prev->mux.unlock();
                    }
                    it->mux.unlock();
                    if(it->next) {
                        it->next->mux.unlock();
                    }
                    it = it->next;
                }
            }
        }

        // Lookup function
        void lookup(int key, std::string& val) {
            auto it = map[key%CAPACITY];
            while (it){
                it->mux.lock();
                if (it->key == key && it->valid){
                    val = it->value;
                    it->mux.unlock();
                    return;
                } else {
                    it->mux.unlock();
                    it = it->next;
                }
            }
        }
};

class LockFreeKeyValueStore {
    private:
    LinkedListNode** map;
    public:
        LockFreeKeyValueStore() {
            map = new LinkedListNode*[CAPACITY]();
            for (int i = 0; i < CAPACITY; i++){
                auto node = new LinkedListNode(-1, "");
                map[i] = node;
            }
        }
        // Insertion function
        void insert(int key, const std::string& value) {
            auto node = new LinkedListNode(key, value);
            node->valid = 1;
            auto it = map[key%CAPACITY];
            while(1) {
                while(it->next){
                    it = it->next;
                }
                node->prev = it;
                if (__sync_val_compare_and_swap(&(it->next), NULL, node) == NULL){
                    return;
                }
            }
        }

        void update(int key, const std::string& value){
            auto it = map[key%CAPACITY];
            char* new_val = new char[MAXLEN+1]();
            strcpy(new_val, value.c_str());
            while (it){
                if (it->key == key && it->valid){
                    while(1) {
                        auto old_val = it->value;
                        if (__sync_val_compare_and_swap(&(it->value), old_val, new_val) == old_val){
                            delete[] old_val;
                            return;
                        }
                    }
                } else {
                    it = it->next;
                }
            }
        }
        // Deletion function
        void remove(int key) {
            auto it = map[key%CAPACITY];
            while (it){
                if (it->key == key && it->valid){
                    while(1) {
                        auto old_val = it->valid;
                        if (__sync_val_compare_and_swap(&(it->valid), old_val, 0) == old_val){
                            return;
                        }
                    }
                } else {
                    it = it->next;
                }
            }
        }

        // Lookup function
        void lookup(int key, std::string& val) {
            auto it = map[key%CAPACITY];
            while (it){
                if (it->key == key && it->valid){
                    val = it->value;
                    return;
                } else {
                    it = it->next;
                }
            }
        }
};

void concurrentCoarseGrainedTests(CoarseGrainedKeyValueStore& kvStore, int numThreads, int numIterations) {
    std::vector<std::thread> threads_insert;

    for (int i = 0; i < numThreads; ++i) {
        threads_insert.emplace_back([&kvStore, i, numIterations]() {
            for (int j = 0; j < numIterations; ++j) {
                auto toInsert = "Value" + std::to_string(i * numIterations + j);
                kvStore.insert(i * numIterations + j, toInsert);
            }
        });
    }

    auto start_time_insert = std::chrono::high_resolution_clock::now();
    // Join all threads
    for (auto& thread : threads_insert) {
        thread.join();
    }

    // End timing
    auto end_time_insert = std::chrono::high_resolution_clock::now();

    // Calculate the duration in microseconds
    auto duration_insert = std::chrono::duration_cast<std::chrono::microseconds>(end_time_insert - start_time_insert);

    // Output the duration
    std::cout << "CG Insertion Map Time taken: " << duration_insert.count() << " microseconds." << std::endl;

    // Verify that all values were inserted
    for (int i = 0; i < numThreads * numIterations; ++i) {
        std::string str_ptr = "";
        kvStore.lookup(i, str_ptr);
        if (str_ptr != "Value"+std::to_string(i)){
            std::cout << std::to_string(i) + " Result is " + str_ptr << std::endl;
        }
        assert(str_ptr == "Value"+std::to_string(i)); // Assert that the value is not empty
    }
    std::cout << "CONCURRENT INSERT: All tests passed!" << std::endl;

    std::vector<std::thread> threads_update;

    for (int i = 0; i < numThreads; ++i) {
        threads_update.emplace_back([&kvStore, i, numIterations]() {
            for (int j = 0; j < numIterations; ++j) {
                auto toInsert = "Value" + std::to_string((i * numIterations + j)*2);
                kvStore.update(i * numIterations + j, toInsert);
            }
        });
    }

    auto start_time_update = std::chrono::high_resolution_clock::now();
    // Join all threads
    for (auto& thread : threads_update) {
        thread.join();
    }

    // End timing
    auto end_time_update = std::chrono::high_resolution_clock::now();

    // Calculate the duration in microseconds
    auto duration_update = std::chrono::duration_cast<std::chrono::microseconds>(end_time_update - start_time_update);

    // Output the duration
    std::cout << "CG UPDATE Map Time taken: " << duration_update.count() << " microseconds." << std::endl;

    // Verify that all values were inserted
    for (int i = 0; i < numThreads * numIterations; ++i) {
        std::string str_ptr = "";
        kvStore.lookup(i, str_ptr);
        if (str_ptr != "Value"+std::to_string(i*2)){
            std::cout << std::to_string(i) + " Result is " + str_ptr << std::endl;
        }

        assert(str_ptr == "Value"+std::to_string(i*2)); // Assert that the value is not empty
    }

    std::cout << "CONCURRENT UPDATE: All tests passed!" << std::endl;
    
    std::vector<std::thread> threads_remove;

    for (int i = 0; i < numThreads; ++i) {
        threads_remove.emplace_back([&kvStore, i, numIterations]() {
            for (int j = 0; j < numIterations; ++j) {
                kvStore.remove(i * numIterations + j);
            }
        });
    }

    auto start_time_remove = std::chrono::high_resolution_clock::now();
    // Join all threads
    for (auto& thread : threads_remove) {
        thread.join();
    }

    // End timing
    auto end_time_remove = std::chrono::high_resolution_clock::now();

    // Calculate the duration in microseconds
    auto duration_remove = std::chrono::duration_cast<std::chrono::microseconds>(end_time_remove - start_time_remove);

    // Output the duration
    std::cout << "CG DELETE Map Time taken: " << duration_remove.count() << " microseconds." << std::endl;

    // Verify that all values were deleted
    for (int i = 0; i < numThreads * numIterations; ++i) {
        std::string str_ptr = "";
        kvStore.lookup(i, str_ptr);
       if (str_ptr != ""){
            std::cout << std::to_string(i) + " Result is " + str_ptr << std::endl;
        }
        assert(str_ptr == ""); // Assert that the value is not empty
    }
    std::cout << "CONCURRENT DELETE: All tests passed!" << std::endl;
}

void concurrentFineGrainedTests(FineGrainedKeyValueStore& kvStore, int numThreads, int numIterations) {
    std::vector<std::thread> threads_insert;

    for (int i = 0; i < numThreads; ++i) {
        threads_insert.emplace_back([&kvStore, i, numIterations]() {
            for (int j = 0; j < numIterations; ++j) {
                auto toInsert = "Value" + std::to_string(i * numIterations + j);
                kvStore.insert(i * numIterations + j, toInsert);
            }
        });
    }

    auto start_time_insert = std::chrono::high_resolution_clock::now();
    // Join all threads
    for (auto& thread : threads_insert) {
        thread.join();
    }

    // End timing
    auto end_time_insert = std::chrono::high_resolution_clock::now();

    // Calculate the duration in microseconds
    auto duration_insert = std::chrono::duration_cast<std::chrono::microseconds>(end_time_insert - start_time_insert);

    // Output the duration
    std::cout << "CG Insertion Map Time taken: " << duration_insert.count() << " microseconds." << std::endl;

    // Verify that all values were inserted
    for (int i = 0; i < numThreads * numIterations; ++i) {
        std::string str_ptr = "";
        kvStore.lookup(i, str_ptr);
        if (str_ptr != "Value"+std::to_string(i)){
            std::cout << std::to_string(i) + " Result is " + str_ptr << std::endl;
        }
        assert(str_ptr == "Value"+std::to_string(i)); // Assert that the value is not empty
    }
    std::cout << "CONCURRENT INSERT: All tests passed!" << std::endl;

    std::vector<std::thread> threads_update;

    for (int i = 0; i < numThreads; ++i) {
        threads_update.emplace_back([&kvStore, i, numIterations]() {
            for (int j = 0; j < numIterations; ++j) {
                auto toInsert = "Value" + std::to_string((i * numIterations + j)*2);
                kvStore.update(i * numIterations + j, toInsert);
            }
        });
    }

    auto start_time_update = std::chrono::high_resolution_clock::now();
    // Join all threads
    for (auto& thread : threads_update) {
        thread.join();
    }

    // End timing
    auto end_time_update = std::chrono::high_resolution_clock::now();

    // Calculate the duration in microseconds
    auto duration_update = std::chrono::duration_cast<std::chrono::microseconds>(end_time_update - start_time_update);

    // Output the duration
    std::cout << "CG UPDATE Map Time taken: " << duration_update.count() << " microseconds." << std::endl;

    // Verify that all values were inserted
    for (int i = 0; i < numThreads * numIterations; ++i) {
        std::string str_ptr = "";
        kvStore.lookup(i, str_ptr);
        if (str_ptr != "Value"+std::to_string(i*2)){
            std::cout << std::to_string(i) + " Result is " + str_ptr << std::endl;
        }

        assert(str_ptr == "Value"+std::to_string(i*2)); // Assert that the value is not empty
    }

    std::cout << "CONCURRENT UPDATE: All tests passed!" << std::endl;
    
    std::vector<std::thread> threads_remove;

    for (int i = 0; i < numThreads; ++i) {
        threads_remove.emplace_back([&kvStore, i, numIterations]() {
            for (int j = 0; j < numIterations; ++j) {
                kvStore.remove(i * numIterations + j);
            }
        });
    }

    auto start_time_remove = std::chrono::high_resolution_clock::now();
    // Join all threads
    for (auto& thread : threads_remove) {
        thread.join();
    }

    // End timing
    auto end_time_remove = std::chrono::high_resolution_clock::now();

    // Calculate the duration in microseconds
    auto duration_remove = std::chrono::duration_cast<std::chrono::microseconds>(end_time_remove - start_time_remove);

    // Output the duration
    std::cout << "CG DELETE Map Time taken: " << duration_remove.count() << " microseconds." << std::endl;

    // Verify that all values were deleted
    for (int i = 0; i < numThreads * numIterations; ++i) {
        std::string str_ptr = "";
        kvStore.lookup(i, str_ptr);
       if (str_ptr != ""){
            std::cout << std::to_string(i) + " Result is " + str_ptr << std::endl;
        }
        assert(str_ptr == ""); // Assert that the value is not empty
    }
    std::cout << "CONCURRENT DELETE: All tests passed!" << std::endl;
}

void concurrentLockFreeTests(LockFreeKeyValueStore& kvStore, int numThreads, int numIterations) {
    std::vector<std::thread> threads_insert;

    for (int i = 0; i < numThreads; ++i) {
        threads_insert.emplace_back([&kvStore, i, numIterations]() {
            for (int j = 0; j < numIterations; ++j) {
                auto toInsert = "Value" + std::to_string(i * numIterations + j);
                kvStore.insert(i * numIterations + j, toInsert);
            }
        });
    }

    auto start_time_insert = std::chrono::high_resolution_clock::now();
    // Join all threads
    for (auto& thread : threads_insert) {
        thread.join();
    }

    // End timing
    auto end_time_insert = std::chrono::high_resolution_clock::now();

    // Calculate the duration in microseconds
    auto duration_insert = std::chrono::duration_cast<std::chrono::microseconds>(end_time_insert - start_time_insert);

    // Output the duration
    std::cout << "CG Insertion Map Time taken: " << duration_insert.count() << " microseconds." << std::endl;

    // Verify that all values were inserted
    for (int i = 0; i < numThreads * numIterations; ++i) {
        std::string str_ptr = "";
        kvStore.lookup(i, str_ptr);
        if (str_ptr != "Value"+std::to_string(i)){
            std::cout << std::to_string(i) + " Result is " + str_ptr << std::endl;
        }
        assert(str_ptr == "Value"+std::to_string(i)); // Assert that the value is not empty
    }
    std::cout << "CONCURRENT INSERT: All tests passed!" << std::endl;

    std::vector<std::thread> threads_update;

    for (int i = 0; i < numThreads; ++i) {
        threads_update.emplace_back([&kvStore, i, numIterations]() {
            for (int j = 0; j < numIterations; ++j) {
                auto toInsert = "Value" + std::to_string((i * numIterations + j)*2);
                kvStore.update(i * numIterations + j, toInsert);
            }
        });
    }

    auto start_time_update = std::chrono::high_resolution_clock::now();
    // Join all threads
    for (auto& thread : threads_update) {
        thread.join();
    }

    // End timing
    auto end_time_update = std::chrono::high_resolution_clock::now();

    // Calculate the duration in microseconds
    auto duration_update = std::chrono::duration_cast<std::chrono::microseconds>(end_time_update - start_time_update);

    // Output the duration
    std::cout << "CG UPDATE Map Time taken: " << duration_update.count() << " microseconds." << std::endl;

    // Verify that all values were inserted
    for (int i = 0; i < numThreads * numIterations; ++i) {
        std::string str_ptr = "";
        kvStore.lookup(i, str_ptr);
        if (str_ptr != "Value"+std::to_string(i*2)){
            std::cout << std::to_string(i) + " Result is " + str_ptr << std::endl;
        }

        assert(str_ptr == "Value"+std::to_string(i*2)); // Assert that the value is not empty
    }

    std::cout << "CONCURRENT UPDATE: All tests passed!" << std::endl;
    
    std::vector<std::thread> threads_remove;

    for (int i = 0; i < numThreads; ++i) {
        threads_remove.emplace_back([&kvStore, i, numIterations]() {
            for (int j = 0; j < numIterations; ++j) {
                kvStore.remove(i * numIterations + j);
            }
        });
    }

    auto start_time_remove = std::chrono::high_resolution_clock::now();
    // Join all threads
    for (auto& thread : threads_remove) {
        thread.join();
    }

    // End timing
    auto end_time_remove = std::chrono::high_resolution_clock::now();

    // Calculate the duration in microseconds
    auto duration_remove = std::chrono::duration_cast<std::chrono::microseconds>(end_time_remove - start_time_remove);

    // Output the duration
    std::cout << "CG DELETE Map Time taken: " << duration_remove.count() << " microseconds." << std::endl;

    // Verify that all values were deleted
    for (int i = 0; i < numThreads * numIterations; ++i) {
        std::string str_ptr = "";
        kvStore.lookup(i, str_ptr);
       if (str_ptr != ""){
            std::cout << std::to_string(i) + " Result is " + str_ptr << std::endl;
        }
        assert(str_ptr == ""); // Assert that the value is not empty
    }
    std::cout << "CONCURRENT DELETE: All tests passed!" << std::endl;
}

int main() {
    auto start_time = std::chrono::high_resolution_clock::now();

    auto kvStoreCoarseGrained = CoarseGrainedKeyValueStore();
    auto kvStoreFineGrained = FineGrainedKeyValueStore();
    auto kvStoreLockFree = LockFreeKeyValueStore();

    std::cout << "RUNNING COARSE GRAINED TESTS" << std::endl;
    concurrentCoarseGrainedTests(kvStoreCoarseGrained, 10, 5000);
    std::cout << "RUNNING FINE GRAINED TESTS" << std::endl;
    concurrentFineGrainedTests(kvStoreFineGrained, 10, 5000);
    std::cout << "RUNNING LOCK FREE TESTS" << std::endl;
    concurrentLockFreeTests(kvStoreLockFree, 10, 5000);

    std::cout << "All tests passed!" << std::endl;
     // End timing
    auto end_time = std::chrono::high_resolution_clock::now();

    // Calculate the duration in microseconds
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

    // Output the duration
    std::cout << "Total Test Time taken: " << duration.count() << " microseconds." << std::endl;
    return 0;
}



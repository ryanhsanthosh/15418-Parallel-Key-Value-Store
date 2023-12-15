#include <iostream>
#include <map>
#include <cassert>
#include <thread>
#include <mutex>
#include <vector>
#include <unordered_map>
#include<string.h>
#define UINT_MAX 9999999
#define THREADING 1 //this is used to check timing and debugging
/**
 * @Brief: 
 * In this file, we are implementing a fine-grained lock version of the key value
 * store, using a hashmap here. 
 * 
 * Our hashmap takes in an integer value as a key and a valueObject as the value.
 * Each valueObject is consisted of a string value and a mutex.
 * 
 * We also have a global mutex which will be used when accessing the hashmap.
 * 
 * In our implementation, we aim to enable and maximize parallelism in our data
 * structure operations, reducing contention for global data structure lock, globalMutex, here.
 * 
 * By introducing a mutex for each key-value pair, we hope to release the globalMutex lock 
 * earlier than before.
 * 
 * Please note that unlocks are implicit within the program, unless coded.
 * */

  class value_t{
    public:
        std::string value;
        std::mutex valueMutex;

        //init
        value_t(std::string val){
            value = val;
        }
        value_t(){

        }
        //copy constructor
        value_t(value_t& input){
            value = input.value;

        }

        value_t& operator=(const value_t& input){
            value = input.value;
            return *this;
        }
    };


std::mutex globalMutex;
std::map<int, std::string> data; // Using int as key type for simplicity

std::mutex valueMutexes[UINT_MAX];


class FineGrainedKeyValueStore {

 
    public:
        // Insertion function
        void insert(int key, std::string value) {
            
            //check if value's already in there
            //if it's already in there.. just ignore it

            //std::unique_lock<std::mutex> lock(globalMutex);
            if (THREADING == 1){
                globalMutex.lock();
                //valueMutexes[key].lock();
            }
            
            auto it = data.find(key);
            //doesn't have
            if (it == data.end()) {
                //lock.unlock();
               
                data[key] = value; //SEG FAULT HAPPENS HERE WTF
                globalMutex.unlock(); //NEEDS TO BE AFTER DATA[KEY] OR ELSE VALUE NOT ADDED.
                
                //valueMutexes[key].unlock();
                return;
            } 
            
            if (THREADING == 1){
                //std::lock_guard<std::mutex> lock(globalMutex);
                globalMutex.unlock();
               // valueMutexes[key].unlock();
            }
            
           
        }

          // Deletion function
        void remove(int key) {
 
             //std::cout << "remove1" << key << std::endl;
             if (THREADING == 1){
                globalMutex.lock();
               // valueMutexes[key].lock(); for optimization
            }
            //std::cout << "remove1 here" << key << std::endl;
            auto it = data.find(key);
            if (it != data.end()) {
                data.erase(key);
                globalMutex.unlock();
                //want to unlock global mutex here.. but will cause sf
                //std::cout << "about to erase here" << key << std::endl;
                
                //valueMutexes[key].unlock();
                //std::cout << "done erase here" << key << std::endl;
                return;
            } 
            //unlock
            
            if (THREADING == 1){
          
                globalMutex.unlock();
                //valueMutexes[key].unlock();
            }
            
           
        }

        void update(int key, std::string value){
            //add lock
            //std::unique_lock<std::mutex> lock(globalMutex);
            if (THREADING == 1){
                globalMutex.lock();
                valueMutexes[key].lock();
            }
            auto it = data.find(key);
            if (it != data.end()) {
                globalMutex.unlock();
                data[key] = value;
                valueMutexes[key].unlock();
                return;
            }

            //unlock
            
            if (THREADING == 1){
                //std::lock_guard<std::mutex> lock(globalMutex);
                globalMutex.unlock();
                valueMutexes[key].unlock();
            }
            
    
        }
      
        // Lookup function
        void lookup(int key, std::string& val) { //will store in val
            //mutex already locked
            if (THREADING == 1){
                globalMutex.lock();
                valueMutexes[key].lock();
            }
            //std::cout << "LU1" << key << std::endl;

            auto it = data.find(key);
            if (it != data.end()) {
                globalMutex.unlock();
                val = data[key]; //SEG FAULT
                valueMutexes[key].unlock();
                return;
                //std::cout << "LU3" << key << std::endl;
               
               
            } 
               // std::cout << "Key not found: " << key << std::endl;
                //lock.unlock();
                
            if (THREADING == 1){
                //std::lock_guard<std::mutex> lock(globalMutex);
                globalMutex.unlock();
                valueMutexes[key].unlock();

            }
                
            

        }
};

//each thread is writing to each index
void concurrentInsertTest(FineGrainedKeyValueStore& kvStore, int numThreads, int numIterations) {
    auto start_time = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> threads;

    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([&kvStore, i, numIterations]() {
            for (int j = 0; j < numIterations; ++j) {
                value_t obj;
                auto toInsert = "Value" + std::to_string(i * numIterations + j);
                obj.value = toInsert;
                kvStore.insert(i * numIterations + j, toInsert);
            }
        });
    }

    // Join all threads
    try {
    // Join all threads
        for (auto& thread : threads) {
            thread.join();
        }
    } catch (const std::exception& e) {
        std::cerr << "Exception caught: " << e.what() << std::endl;
    }

    // Calculate the duration in microseconds
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    std::cout << "INSERT FineGrained Map Time taken: " << duration.count() << " microseconds." << std::endl;

    // Verify that all values were inserted
    for (int i = 0; i < numThreads * numIterations; ++i) {
        //std::cout << "verifying vals" << std::endl;
        std::string str_ptr = "";
        assert(data[i] == "Value"+std::to_string(i));
        //std::cout << str_ptr << std::endl;
        /*
        if (str_ptr != "Value"+std::to_string(i)){
            std::cout << std::to_string(i) + " Result is " +  str_ptr<< std::endl;
        }
        */

        //assert(str_ptr == "Value"+std::to_string(i)); // Assert that the value is not empty
    }
}


//threads are writing to the same index if their (%5) is the same
//we have a consistent load balance here
void concurrentInsertTest_nKeys(FineGrainedKeyValueStore& kvStore, int numThreads, int numIterations, int keys) {
    auto start_time = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> threads;

    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([&kvStore, i, numIterations, keys]() {
            for (int j = 0; j < numIterations; ++j) {
                value_t obj;
                auto toInsert = "Value" + std::to_string(i * numIterations + j);
                obj.value = toInsert;
                kvStore.insert(((i * numIterations + j) % keys), toInsert);
            }
        });
    }

    // Join all threads
    try {
    // Join all threads
        for (auto& thread : threads) {
            thread.join();
        }
    } catch (const std::exception& e) {
        std::cerr << "Exception caught: " << e.what() << std::endl;
    }

    // Calculate the duration in microseconds
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    std::cout << "INSERT "<< keys << " keys FineGrained Map Time taken: " << duration.count() << " microseconds." << std::endl;
}



//threads are updating to the same index if their (%5) is the same
//we have a consistent load balance here
void concurrentUpdateTest_nKeys(FineGrainedKeyValueStore& kvStore, int numThreads, int numIterations, int k) {
    auto start_time = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> threads;

    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([&kvStore, i, numIterations, k]() {
            for (int j = 0; j < numIterations; ++j) {
                value_t obj;
                auto toInsert = "Value" + std::to_string(i * numIterations + j);
                obj.value = toInsert;
                kvStore.update((i * numIterations + j) % k, toInsert);
            }
        });
    }

    // Join all threads
    try {
    // Join all threads
        for (auto& thread : threads) {
            thread.join();
        }
    } catch (const std::exception& e) {
        std::cerr << "Exception caught: " << e.what() << std::endl;
    }

    // Calculate the duration in microseconds
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
     std::cout << "UPDATE "<< k << " keys FineGrained Map Time taken: " << duration.count() << " microseconds." << std::endl;
}


void concurrentUpdateTest(FineGrainedKeyValueStore& kvStore, int numThreads, int numIterations) {
    auto start_time = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> threads;

    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([&kvStore, i, numIterations]() {
            for (int j = 0; j < numIterations; ++j) {
                value_t obj;
                auto toInsert = "Value" + std::to_string((i * numIterations + j)*2);
                obj.value = toInsert;
                //obj.valueMutex = 
                kvStore.update(i * numIterations + j, toInsert);
            }
        });
    }

    // Join all threads
    for (auto& thread : threads) {
        thread.join();
    }

     // Calculate the duration in microseconds
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    std::cout << "UPDATE FineGrained Map Time taken: " << duration.count() << " microseconds." << std::endl;

    // Verify that all values were inserted
    for (int i = 0; i < numThreads * numIterations; ++i) {
        std::string str_ptr = "";
        kvStore.lookup(i, str_ptr);
        //std::cout << std::to_string(i) + " Result is " + result << std::endl;

        assert(str_ptr == "Value"+std::to_string(i*2)); // Assert that the value is not empty
    }
}

void concurrentRemoveTest(FineGrainedKeyValueStore& kvStore, int numThreads, int numIterations) {
    
    auto start_time = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;


    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([&kvStore, i, numIterations]() {
            for (int j = 0; j < numIterations; ++j) {
                //std::cout << "here in delete 1" << std::endl;
                //std::cout << "trying to remove " + std::to_string(i * numIterations + j) << std::endl;
                kvStore.remove(i * numIterations + j);
            }
        });
    }

    // Join all threads
    for (auto& thread : threads) {
        thread.join();
    }
    auto end_time = std::chrono::high_resolution_clock::now();
      // Calculate the duration in microseconds
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    std::cout << "Delete FineGrained Map Time taken: " << duration.count() << " microseconds." << std::endl;
    // Verify that all values were deleted
    for (int i = 0; i < numThreads * numIterations; ++i) {
        std::string str_ptr = "";
        kvStore.lookup(i, str_ptr);
       // std::cout << "here in delete" << std::endl;

        assert(str_ptr == ""); // Assert that the value is not empty
    }
}

//threads are removing to the same index if their (%5) is the same
//we have a consistent load balance here
void concurrentRemoveTest_nKeys(FineGrainedKeyValueStore& kvStore, int numThreads, int numIterations, int k) {
    auto start_time = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> threads;

    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([&kvStore, i, numIterations, k]() {
            for (int j = 0; j < numIterations; ++j) {
                value_t obj;
                auto toInsert = "Value" + std::to_string(i * numIterations + j);
                obj.value = toInsert;
                kvStore.remove(((i * numIterations + j) % k));
            }
        });
    }

    // Join all threads
    try {
    // Join all threads
        for (auto& thread : threads) {
            thread.join();
        }
    } catch (const std::exception& e) {
        std::cerr << "Exception caught: " << e.what() << std::endl;
    }

    // Calculate the duration in microseconds
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    std::cout << "Remove " << k << " keys FineGrained Map Time taken: " << duration.count() << " microseconds." << std::endl;
}

//threads are removing to the same index if their (%5) is the same
//we have a consistent load balance here
void concurrentLookUpTest_nKeys(FineGrainedKeyValueStore& kvStore, int numThreads, int numIterations, int k) {
    auto start_time = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> threads;

    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([&kvStore, i, numIterations, k]() {
            for (int j = 0; j < numIterations; ++j) {
                value_t obj;
                auto toInsert = "Value" + std::to_string(i * numIterations + j);
                obj.value = toInsert;
                std::string str_ptr = "";
       
                kvStore.lookup(((i * numIterations + j) % k), str_ptr);
            }
        });
    }

    // Join all threads
    try {
    // Join all threads
        for (auto& thread : threads) {
            thread.join();
        }
    } catch (const std::exception& e) {
        std::cerr << "Exception caught: " << e.what() << std::endl;
    }

    // Calculate the duration in microseconds
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    std::cout << "Lookup " << k << " keys FineGrained Map Time taken: " << duration.count() << " microseconds." << std::endl;
}

int main() {

    
    FineGrainedKeyValueStore kvStore;
    //(1000, 1000) better than seq
    
    //(500, 2000) worse than seq

    //(100, 1000) better than seq
    //(10, 1000) -> seq better
    //(1, 1000) -> seq better
    
    int numThreads = 500;
    int numIters = 1000;
    // Run the concurrent insert test
    concurrentInsertTest(kvStore, numThreads, numIters);
    std::cout << "CONCURRENT INSERT: All tests passed!" << std::endl;

    // Run the concurrent insert test
    concurrentUpdateTest(kvStore, numThreads, numIters);
    std::cout << "CONCURRENT UPDATE: All tests passed!" << std::endl;

    //Run the concurrent delete tests
    concurrentRemoveTest(kvStore, numThreads, numIters);
    std::cout << "CONCURRENT DELETE: All tests passed!" << std::endl;

    auto start_time = std::chrono::high_resolution_clock::now();
    for (int i = 1; i < numThreads * numIters * 10; i *= 10){
        concurrentInsertTest_nKeys(kvStore, numThreads, numIters, i);
    }

    for (int i = 1; i < numThreads * numIters * 10; i *= 10){
        concurrentUpdateTest_nKeys(kvStore, numThreads, numIters, i);
    }

     for (int i = 1; i < numThreads * numIters * 10; i *= 10){
        concurrentLookUpTest_nKeys(kvStore, numThreads, numIters, i);
    }

    for (int i = 1; i < numThreads * numIters * 10; i *= 10){
        concurrentRemoveTest_nKeys(kvStore, numThreads, numIters, i);
    }
    

    std::cout << "All tests passed!" << std::endl;
    auto end_time = std::chrono::high_resolution_clock::now();

    // Calculate the duration in microseconds
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

    // Output the duration
    std::cout << "FineGrained Map Time taken: " << duration.count() << " microseconds." << std::endl;
    return 0;
}

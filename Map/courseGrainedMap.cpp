#include <iostream>
#include <map>
#include <cassert>
#include <thread>
#include <mutex>
#include <vector>
#include <unordered_map>
#include<string.h>
#define THREADING 1 //this is used to check timing and debugging
/**
 * @Brief: 
 * In this file, we are implementing a coarse-grained lock version of the key value
 * store, using a hashmap here. 
 * 
 * Our global hashmap takes in an integer value as a key and a string value as a value.
 * 
 * We have a global mutex which will be used when accessing the hashmap.
 * 
 * In this implementation, we will lock our mutex from when we access our global data structure,
 * the hashmap, and will release it once we are done reading to/writing from the hashmap.
 * 
 * Please note that unlocks are implicit within the program, unless coded.
 * */

std::mutex globalMutex;
std::map<int, std::string> data; // Using int as key type for simplicity

class CoarseGrainedKeyValueStore {

    public:
        // Insertion function
        void insert(int key, const std::string& value) {

            
            if (THREADING == 1){
                //std::lock_guard<std::mutex> lock(globalMutex);
                globalMutex.lock();
            }
            
          
            auto it = data.find(key);
            if (it == data.end()) {
                //std::cout << "key " + std::to_string(key) + " found " + value << std::endl;
                data[key] = value;
            } 
            
           
            //unlock
            if (THREADING == 1){
                //std::lock_guard<std::mutex> lock(globalMutex);
                globalMutex.unlock();
            }
           
        }

        void update(int key, const std::string& value){
            //add lock
            //std::unique_lock<std::mutex> lock;
            
            if (THREADING == 1){
                //std::unique_lock<std::mutex> lock(globalMutex);
                globalMutex.lock();
            }

            auto it = data.find(key);
            if (it != data.end()) {
                data[key] = value;
            } 
            if (THREADING == 1){
                //std::lock_guard<std::mutex> lock(globalMutex);
                globalMutex.unlock();
            }
            //unlock
    
        }
        // Deletion function
        void remove(int key) {
            //add lock
            //std::unique_lock<std::mutex> lock;
             if (THREADING == 1){
                //std::unique_lock<std::mutex> lock(globalMutex);
                globalMutex.lock();
            }
            auto it = data.find(key);
            if (it != data.end()) {
                data.erase(it);
            } 

            if (THREADING == 1){
                //std::lock_guard<std::mutex> lock(globalMutex);
                globalMutex.unlock();
            }
            //unlock
            
          
        }

        //NOT ADDING LOCK HERE BECAUSE WANT TO UNLOCK AFTER RETURNING
        // Lookup function
        void lookup(int key, std::string& val) {
            

            if (THREADING == 1){
                //std::unique_lock<std::mutex> lock(globalMutex);
                globalMutex.lock();
            }
           
            auto it = data.find(key);
            if (it != data.end()) {
                //std::cout << "Key: " << key << ", Value: " << it->second << std::endl;
                val = data[key];
                //not sure if this would work in case sth else changes it
            } 
                //std::cout << "Key not found: " << key << std::endl;
            if (THREADING == 1){
                //std::lock_guard<std::mutex> lock(globalMutex);
                globalMutex.unlock();
            }

        }
};

void concurrentInsertTest(CoarseGrainedKeyValueStore& kvStore, int numThreads, int numIterations) {
    
    std::vector<std::thread> threads;
    auto start_time = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([&kvStore, i, numIterations]() {
            for (int j = 0; j < numIterations; ++j) {
                auto toInsert = "Value" + std::to_string(i * numIterations + j);
                //obj.valueMutex = 
                kvStore.insert(i * numIterations + j, toInsert);
            }
        });
    }

    // Join all threads
    for (auto& thread : threads) {
        thread.join();
    }

    // End timing
    auto end_time = std::chrono::high_resolution_clock::now();

    // Calculate the duration in microseconds
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

    // Output the duration
    std::cout << "CG Insertion Map Time taken: " << duration.count() << " microseconds." << std::endl;

    // Verify that all values were inserted
    for (int i = 0; i < numThreads * numIterations; ++i) {
        std::string str_ptr = "";
        kvStore.lookup(i, str_ptr);
        //std::cout << std::to_string(i) + " Result is " + result << std::endl;
        if (str_ptr != "Value"+std::to_string(i)){
            std::cout << std::to_string(i) + " Result is " + str_ptr << std::endl;
        }
        assert(str_ptr == "Value"+std::to_string(i)); // Assert that the value is not empty
    }
}

void concurrentRemoveTest(CoarseGrainedKeyValueStore& kvStore, int numThreads, int numIterations) {
    
    auto start_time = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> threads;

    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([&kvStore, i, numIterations]() {
            for (int j = 0; j < numIterations; ++j) {
                //std::cout << "here in delete 1" << std::endl;
                kvStore.remove(i * numIterations + j);
            }
        });
    }

    // Join all threads
    for (auto& thread : threads) {
        thread.join();
    }

    // End timing
    auto end_time = std::chrono::high_resolution_clock::now();

    // Calculate the duration in microseconds
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

    // Output the duration
    std::cout << "CG DELETE Map Time taken: " << duration.count() << " microseconds." << std::endl;

    // Verify that all values were deleted
    for (int i = 0; i < numThreads * numIterations; ++i) {
        std::string str_ptr = "";
        kvStore.lookup(i, str_ptr);
       // std::cout << "here in delete" << std::endl;

        assert(str_ptr == ""); // Assert that the value is not empty
    }
}

void concurrentUpdateTest(CoarseGrainedKeyValueStore& kvStore, int numThreads, int numIterations) {
    auto start_time = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> threads;

    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([&kvStore, i, numIterations]() {
            for (int j = 0; j < numIterations; ++j) {
               
                auto toInsert = "Value" + std::to_string((i * numIterations + j)*2);
              
                //obj.valueMutex = 
                kvStore.update(i * numIterations + j, toInsert);
            }
        });
    }

    // Join all threads
    for (auto& thread : threads) {
        thread.join();
    }

    // End timing
    auto end_time = std::chrono::high_resolution_clock::now();

    // Calculate the duration in microseconds
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

    // Output the duration
    std::cout << "CG UPDATE Map Time taken: " << duration.count() << " microseconds." << std::endl;

    // Verify that all values were inserted
    for (int i = 0; i < numThreads * numIterations; ++i) {
        std::string str_ptr = "";
        kvStore.lookup(i, str_ptr);
        //std::cout << std::to_string(i) + " Result is " + result << std::endl;

        assert(str_ptr == "Value"+std::to_string(i*2)); // Assert that the value is not empty
    }
}

//threads are writing to the same index if their (%5) is the same
//we have a consistent load balance here
void concurrentInsertTest_nKeys(CoarseGrainedKeyValueStore& kvStore, int numThreads, int numIterations, int keys) {
    auto start_time = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> threads;

    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([&kvStore, i, numIterations, keys]() {
            for (int j = 0; j < numIterations; ++j) {
               
                auto toInsert = "Value" + std::to_string(i * numIterations + j);
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
    std::cout << "INSERT "<< keys << " keys CGrained Map Time taken: " << duration.count() << " microseconds." << std::endl;
}

//threads are updating to the same index if their (%5) is the same
//we have a consistent load balance here
void concurrentUpdateTest_nKeys(CoarseGrainedKeyValueStore& kvStore, int numThreads, int numIterations, int k) {
    auto start_time = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> threads;

    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([&kvStore, i, numIterations, k]() {
            for (int j = 0; j < numIterations; ++j) {
                auto toInsert = "Value" + std::to_string(i * numIterations + j);
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
     std::cout << "UPDATE "<< k << " keys CGrained Map Time taken: " << duration.count() << " microseconds." << std::endl;
}

//threads are removing to the same index if their (%5) is the same
//we have a consistent load balance here
void concurrentRemoveTest_nKeys(CoarseGrainedKeyValueStore& kvStore, int numThreads, int numIterations, int k) {
    auto start_time = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> threads;

    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([&kvStore, i, numIterations, k]() {
            for (int j = 0; j < numIterations; ++j) {
                auto toInsert = "Value" + std::to_string(i * numIterations + j);
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
    std::cout << "Remove " << k << " keys CGrained Map Time taken: " << duration.count() << " microseconds." << std::endl;
}

void concurrentLookUpTest_nKeys(CoarseGrainedKeyValueStore& kvStore, int numThreads, int numIterations, int k) {
    auto start_time = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> threads;

    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([&kvStore, i, numIterations, k]() {
            for (int j = 0; j < numIterations; ++j) {
                
               
                
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
    std::cout << "Lookup " << k << " keys CGrained Map Time taken: " << duration.count() << " microseconds." << std::endl;
}


int main() {

    auto start_time = std::chrono::high_resolution_clock::now();

   
    CoarseGrainedKeyValueStore kvStore;

    // Run the concurrent insert test
    concurrentInsertTest(kvStore, 100, 5000);
    std::cout << "CONCURRENT INSERT: All tests passed!" << std::endl;

    // Run the concurrent insert test
    concurrentUpdateTest(kvStore, 100, 5000);
    std::cout << "CONCURRENT UPDATE: All tests passed!" << std::endl;

    //Run the concurrent delete tests
    concurrentRemoveTest(kvStore,100, 5000);
    std::cout << "CONCURRENT DELETE: All tests passed!" << std::endl;

    for (int i = 5; i < 5000000; i *= 10){
        concurrentInsertTest_nKeys(kvStore, 100, 5000, i);
    }

    for (int i = 5; i < 5000000; i *= 10){
        concurrentUpdateTest_nKeys(kvStore, 100, 5000, i);
    }

     for (int i = 5; i < 5000000; i *= 10){
        concurrentLookUpTest_nKeys(kvStore, 100, 5000, i);
    }

    for (int i = 5; i < 5000000; i *= 10){
        concurrentRemoveTest_nKeys(kvStore, 100, 5000, i);
    }
    std::cout << "All tests passed!" << std::endl;
     // End timing
    auto end_time = std::chrono::high_resolution_clock::now();

    // Calculate the duration in microseconds
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

    // Output the duration
    std::cout << "CoarseGrained Map Time taken: " << duration.count() << " microseconds." << std::endl;
    return 0;
}


/*
testing ideas...
seems like we check for ones that will only pass with sequential test as well...
for performance comparison against sequential: d01, d02, d03, d04, d05


based off of 15213's proxy lab lmao
1.

we write concurrently... have two different threads
then we wait for them to be done...
we read them concurrently... have two different threads..
wait for it to be done..
check the value

2. 
we spawn 6 different threads and write 6 different things..

we wait

then we read out of order than writing.. make sure it works...
2 4 6 

and wait
and then read
1 3 5
then wait

then check all
*/


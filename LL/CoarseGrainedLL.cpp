#include <iostream>
#include <map>
#include <cassert>

#include <thread>
#include <mutex>
#include <vector>
#include <unordered_map>
#include<string.h>


std::mutex globalMutex;

class LinkedListNode {
    public:
        int key;
        std::string value;
        LinkedListNode* next;
        LinkedListNode* prev;
        LinkedListNode(int keyIns, std::string val){
            key = keyIns;
            value = val;
            next = nullptr;
            prev = nullptr;
        }
};


class DLL{
    //OPTIMIZATION IDEA... WHEN SEARCHING MIGHT DO BINARY SEARCH INSTEAD OF LINEAR
    //SEARCH FOR SEARCHING THE VALUES LOLOLOLOL

    public:
        LinkedListNode* head;
        // Insertion function
        DLL(){
            head = nullptr;
        }
        void insert(int key, const std::string& value) {
            //find key first
            //std::unique_lock<std::mutex> lock(globalMutex);
            LinkedListNode* pointer = lookup_ptr(key);
            if (pointer != nullptr){
                //lock.unlock();
                return;
            }

            //make new node
            LinkedListNode* toAdd = new LinkedListNode(key, value);
            toAdd->next = head;
            if (head) {//not null
                head->prev = toAdd;
            }
            head = toAdd;
            //lock.unlock();

        }

        void update(int key, const std::string& value){
           // std::unique_lock<std::mutex> lock(globalMutex);
            LinkedListNode* ptr = lookup_ptr(key);
            if (ptr == nullptr){
                //lock.unlock();
                return;
            }
            ptr->value = value;
           // lock.unlock();
        }


        // Lookup function
        std::string lookup(int key) {
            //std::unique_lock<std::mutex> lock(globalMutex);
            LinkedListNode* ptr = lookup_ptr(key);
            //std::cout << "here3" << std::endl;
            if (ptr == nullptr){
                //lock.unlock();
                return "";
            }
            //lock.unlock();
            return ptr->value;

        }

         // Lookup function
        LinkedListNode* lookup_ptr(int key) {
           // std::unique_lock<std::mutex> lock(globalMutex);
            LinkedListNode* pointer = head;
            while(pointer != nullptr){
                //std::cout << "here4" << std::endl;
                //std::cout << head->value << std::endl;
                
                if (pointer->key == key){
                    //std::cout << "exiting look up ptr w key" << std::endl;
                    //lock.unlock();
                    return pointer;
                }
                //std::cout << "going to ptr next" << std::endl;
                pointer = pointer->next;
            }
            //std::cout << "exiting lookup ptr" << std::endl;
            //lock.unlock();
            return nullptr;
        }

        // Deletion function
        void remove(int key) {
           // std::unique_lock<std::mutex> lock(globalMutex);
            LinkedListNode* pointer = lookup_ptr(key);
            if (pointer == nullptr){
                //lock.unlock();
                return;
            }
            if (pointer == head){
                head = pointer->next;
            }
            if (pointer->prev){
                pointer->prev->next = pointer->next;
            }
            if (pointer->next){
                pointer->next->prev = pointer->prev;
            }
            //lock.unlock();

        }

       
};

DLL doublyLL;
class CoarseGrainedKeyValueStore_LL{
    //OPTIMIZATION IDEA... WHEN SEARCHING MIGHT DO BINARY SEARCH INSTEAD OF LINEAR
    //SEARCH FOR SEARCHING THE VALUES LOLOLOLOL

    public:
        // Insertion function
        void insert(int key, const std::string& value) {
            //find key first
            std::unique_lock<std::mutex> lock(globalMutex);
            doublyLL.insert(key, value);
            //lock.unlock();

        }

        void update(int key, const std::string& value){
            std::unique_lock<std::mutex> lock(globalMutex);
            doublyLL.update(key, value);
           // lock.unlock();
        }


        // Lookup function
        std::string lookup(int key) {
            std::unique_lock<std::mutex> lock(globalMutex);
            return doublyLL.lookup(key);
        }

         // Lookup function
        LinkedListNode* lookup_ptr(int key) {
            std::unique_lock<std::mutex> lock(globalMutex);
            return doublyLL.lookup_ptr(key);
        }

        // Deletion function
        void remove(int key) {
             std::unique_lock<std::mutex> lock(globalMutex);
            doublyLL.remove(key);
            //lock.unlock();

        }

       
};




void concurrentInsertTest(CoarseGrainedKeyValueStore_LL& kvStore, int numThreads, int numIterations) {
    
    std::vector<std::thread> threads;

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

    // Verify that all values were inserted
    for (int i = 0; i < numThreads * numIterations; ++i) {
        std::string result = kvStore.lookup(i);
        //std::cout << std::to_string(i) + " Result is " + result << std::endl;

        assert(result == "Value"+std::to_string(i)); // Assert that the value is not empty
    }
}

void concurrentRemoveTest(CoarseGrainedKeyValueStore_LL& kvStore, int numThreads, int numIterations) {
    
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

    // Verify that all values were deleted
    for (int i = 0; i < numThreads * numIterations; ++i) {
        std::string result = kvStore.lookup(i);
       // std::cout << "here in delete" << std::endl;

        assert(result == ""); // Assert that the value is not empty
    }
}

void concurrentUpdateTest(CoarseGrainedKeyValueStore_LL& kvStore, int numThreads, int numIterations) {
    
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

    // Verify that all values were inserted
    for (int i = 0; i < numThreads * numIterations; ++i) {
        std::string result = kvStore.lookup(i);
        //std::cout << std::to_string(i) + " Result is " + result << std::endl;

        assert(result == "Value"+std::to_string(i*2)); // Assert that the value is not empty
    }
}

int main() {
    CoarseGrainedKeyValueStore_LL kvStore;
    // Run the concurrent insert test
    concurrentInsertTest(kvStore, 5, 10);
    std::cout << "CONCURRENT INSERT: All tests passed!" << std::endl;

    // Run the concurrent insert test
    concurrentUpdateTest(kvStore, 5, 10);
    std::cout << "CONCURRENT UPDATE: All tests passed!" << std::endl;

    //Run the concurrent delete tests
    concurrentRemoveTest(kvStore,5, 10);
    std::cout << "CONCURRENT DELETE: All tests passed!" << std::endl;

    std::cout << "All tests passed!" << std::endl;

    return 0;
}

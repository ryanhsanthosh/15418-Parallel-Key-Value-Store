#include <iostream>
#include <map>
#include <cassert>
#include <thread>
#include <mutex>
#include <vector>
#include <unordered_map>
#include<string.h>




class LinkedListNode {
    public:
        int key;
        std::string value;
        LinkedListNode* next;
        LinkedListNode* prev;
        //added
        std::mutex valueMutex;
        LinkedListNode(int keyIns, std::string val){
            key = keyIns;
            value = val;
            next = nullptr;
            prev = nullptr;
        }

        LinkedListNode(int keyy, std::string val, LinkedListNode* nextt, LinkedListNode* prevv){
            value = val;
            key = keyy;
            next = nextt;
            prev = prevv;
        }

        LinkedListNode(){
        }

        LinkedListNode(LinkedListNode& input){
            value = input.value;
            key = input.key;
            next = input.next;
            prev = input.prev;
        }

        LinkedListNode& operator=(const LinkedListNode& input){
            value = input.value;
            key = input.key;
            next = input.next;
            prev = input.prev;
            return *this;
        }

};


class DLL{
    //OPTIMIZATION IDEA... WHEN SEARCHING MIGHT DO BINARY SEARCH INSTEAD OF LINEAR
    //SEARCH FOR SEARCHING THE VALUES LOLOLOLOL

    public:
        LinkedListNode* head;
        std::mutex DLLMutex;
        // Insertion function
        DLL(){
            head = nullptr;
        }


        //init
        DLL(LinkedListNode* headd){
            head = headd;
        }
    
        //copy constructor
        DLL(DLL& input){
            head = input.head;
        }

        DLL& operator=(const DLL& input){
            head = input.head;
            return *this;
        }

        void insert(int key, std::string toAdd) {
            //find key first
            //std::unique_lock<std::mutex> lock(globalMutex);
            LinkedListNode* pointer = lookup_ptr(key);
            if (pointer != nullptr){
                //lock.unlock();
                return;
            }
            //not sure :(
            LinkedListNode* toAddPtr = new LinkedListNode(key, toAdd);
            
            //make new node
            if (head) {//not null
                std::unique_lock<std::mutex> lock2(head->valueMutex);
                head->prev = toAddPtr;
                std::unique_lock<std::mutex> lock(toAddPtr->valueMutex);
                toAddPtr->next = head;
                head = toAddPtr;
            }
            else{
                std::unique_lock<std::mutex> lock(toAddPtr->valueMutex);
                toAddPtr->next = nullptr;
                head = toAddPtr;
            }
            //lock.unlock();

        }

        void update(int key, std::string value){
           // std::unique_lock<std::mutex> lock(globalMutex);
            //already locks lookup_ptr
            LinkedListNode* ptr = lookup_ptr(key);
            if (ptr == nullptr){
                //lock.unlock();
                return;
            }

            std::unique_lock<std::mutex> lock(ptr->valueMutex);
            ptr->value = value;
            lock.unlock();
        }


        // Lookup function
        std::string lookup(int key) {
           // std::unique_lock<std::mutex> lock(globalMutex);
            LinkedListNode* ptr = lookup_ptr(key);
            //std::cout << "here3" << std::endl;
            if (ptr == nullptr){
                //lock.unlock();
                return "";
            }
            //lock.unlock();
            std::unique_lock<std::mutex> lock(ptr->valueMutex);
            return ptr->value;

        }

         // Lookup function
         //im assuming unlock happens when it goes outta context...
        LinkedListNode* lookup_ptr(int key) {
           
            //inspired by lecture
            LinkedListNode* prev;
            LinkedListNode* cur;

            //lock the access to dll
            std::unique_lock<std::mutex> lock1(DLLMutex);
            cur = head;
            if(cur){
                lock1.unlock();
            }
            while(cur){
                std::unique_lock<std::mutex> lock(cur->valueMutex);
                if (cur->key == key){
                    return cur;
                }
                lock.unlock();
                cur = cur->next;
            }
            //nothing is found
            return nullptr;
        }

        // Deletion function
        void remove(int key) {
           // std::unique_lock<std::mutex> lock(globalMutex);
            LinkedListNode* ptr= lookup_ptr(key);
            if (ptr == nullptr){
                //lock.unlock();
                return;
            }

            std::unique_lock<std::mutex> lock1(ptr->valueMutex);
            

            //want to lock if the values are valid...
            if (ptr->prev){
                std::unique_lock<std::mutex> lock3(ptr->prev->valueMutex);     
            }
            if (ptr->next){
                std::unique_lock<std::mutex> lock4(ptr->next->valueMutex);
            }

            if (ptr->prev){
                ptr->prev->next = ptr->next;
            }
            if (ptr->next){
                ptr->next->prev = ptr->prev;
            }
           
            if (ptr == head){
                head = ptr->next;
            }
            //lock.unlock();

        }

       
};



void concurrentInsertTest(DLL* kvStore, int numThreads, int numIterations) {
    
    std::vector<std::thread> threads;

    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([&kvStore, i, numIterations]() {
            for (int j = 0; j < numIterations; ++j) {
                std::cout << "here1" << std::endl;
                auto toInsert = "Value" + std::to_string(i * numIterations + j);
                //obj.valueMutex = 
                std::cout << "here2" << std::endl;
                kvStore->insert(i * numIterations + j, toInsert);
            }
        });
    }
   
    // Join all threads
    for (auto& thread : threads) {
        thread.join();
    }

     std::cout << "joining threads in insert test" << std::endl;

    // Verify that all values were inserted
    for (int i = 0; i < numThreads * numIterations; ++i) {
        std::string result = kvStore->lookup(i);
        //std::cout << std::to_string(i) + " Result is " + result << std::endl;

        assert(result == "Value"+std::to_string(i)); // Assert that the value is not empty
    }
}

void concurrentRemoveTest(DLL* kvStore, int numThreads, int numIterations) {
    
    std::vector<std::thread> threads;

    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([&kvStore, i, numIterations]() {
            for (int j = 0; j < numIterations; ++j) {
                //std::cout << "here in delete 1" << std::endl;
                kvStore->remove(i * numIterations + j);
            }
        });
    }

    // Join all threads
    for (auto& thread : threads) {
        thread.join();
    }

    // Verify that all values were deleted
    for (int i = 0; i < numThreads * numIterations; ++i) {
        std::string result = kvStore->lookup(i);
       // std::cout << "here in delete" << std::endl;

        assert(result == ""); // Assert that the value is not empty
    }
}

void concurrentUpdateTest(DLL* kvStore, int numThreads, int numIterations) {
    
    std::vector<std::thread> threads;

    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([&kvStore, i, numIterations]() {
            for (int j = 0; j < numIterations; ++j) {
               
                auto toInsert = "Value" + std::to_string((i * numIterations + j)*2);
              
                //obj.valueMutex = 
                kvStore->update(i * numIterations + j, toInsert);
            }
        });
    }

    // Join all threads
    for (auto& thread : threads) {
        thread.join();
    }

    // Verify that all values were inserted
    for (int i = 0; i < numThreads * numIterations; ++i) {
        std::string result = kvStore->lookup(i);
        //std::cout << std::to_string(i) + " Result is " + result << std::endl;

        assert(result == "Value"+std::to_string(i*2)); // Assert that the value is not empty
    }
}

int main() {
    DLL* kvStore = new DLL();
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

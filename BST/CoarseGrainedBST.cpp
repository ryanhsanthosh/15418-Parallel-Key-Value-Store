#include <iostream>
#include <map>
#include <cassert>
#include <thread>
#include <mutex>
#include <vector>
#include <unordered_map>
#include<string.h>

std::mutex globalMutex;

class BSTNode {
    public:
        int key;
        std::string value;
        BSTNode* right;
        BSTNode* left;
        BSTNode(int keyIns, std::string val){
            key = keyIns;
            value = val;
            left = nullptr;
            right = nullptr;
        }
};

class CoarseGrainedKeyValueStore_BST{
    //OPTIMIZATION IDEA... WHEN SEARCHING MIGHT DO BINARY SEARCH INSTEAD OF LINEAR
    //SEARCH FOR SEARCHING THE VALUES LOLOLOLOL

    public:
        BSTNode* root;
        // Insertion function
        CoarseGrainedKeyValueStore_BST(){
            root = nullptr;
        }
        BSTNode* insert(BSTNode* root_at, int key, const std::string& value) {
            //find key first
             //no root
            std::unique_lock<std::mutex> lock(globalMutex);
            if (root == nullptr){
                root = new BSTNode(key, value);
                std::cout << "here3" << std::endl;
                return root;
            }

            if (root_at == nullptr){
                root_at = new BSTNode(key, value);
                std::cout << "here3" << std::endl;
                return root_at;
            }

            std::cout << "here1" << std::endl;
            BSTNode* pointer = lookup_ptr(root_at, key);
            std::cout << "here11" << std::endl;
            if (pointer != nullptr){
                 std::cout << "here2" << std::endl;
                
                return pointer;
            } //already there
            std::cout << "here11111111" << std::endl;


            //there's a root
            if (key < root_at->key){
                std::cout << "here111" << std::endl;
                root_at->left = insert(root_at->left, key, value);
            }
            else if (key > root_at->key){
                std::cout << "here1111" << std::endl;
                root_at->right = insert(root_at->right, key, value);
            }
            return root_at;
        }

        void update(int key, const std::string& value){
            std::unique_lock<std::mutex> lock(globalMutex);
            BSTNode* ptr = lookup_ptr(root, key);
            if (ptr == nullptr){
                return;
            }
            ptr->value = value;
        }

        // Lookup function
        std::string lookup(int key) {
            std::unique_lock<std::mutex> lock(globalMutex);
            BSTNode* ptr = lookup_ptr(root, key);
            //std::cout << "here3" << std::endl;
            if (ptr == nullptr){
                return "";
            }
            return ptr->value;
        }

         // Lookup function
        BSTNode* lookup_ptr(BSTNode* root, int key) {
            std::unique_lock<std::mutex> lock(globalMutex);
            if (root == nullptr || root->key == key){
                return root;
            }

            if (key < root->key){
                return lookup_ptr(root->left, key);
            }
            else{
                return lookup_ptr(root->right, key);
            }
        }

        BSTNode* findMinKeyNode(BSTNode* root_at){
            std::unique_lock<std::mutex> lock(globalMutex);
            BSTNode* toMakeLeftNull;
            while (root_at->left){
                

                //this case is here because we are only using this function to delete
                if (root_at->left->left == nullptr){
                    toMakeLeftNull = root_at;
                }

                root_at = root_at->left;
                
            }
            toMakeLeftNull->left = nullptr;
            return root_at; 
        }


        // Deletion function
        BSTNode* remove(BSTNode* root_at, int key) {
            std::unique_lock<std::mutex> lock(globalMutex);
            if (root == nullptr){
                return nullptr;
            }
            if (root_at == nullptr){
                return nullptr;
            }
            //cant delete sth not already there
            if (lookup_ptr(root_at, key) == nullptr){
                return nullptr;
            }
            //key to be deleted is less than the root's key
            //so the key must be on the left side of the root
            if (key < root_at->key){
                root_at->left = remove(root_at->left, key);
            }
            //key to be deleted is greater than the root's key
            //so the key must be on the right side of the root
            else if (key > root_at->key){
                root_at->right = remove(root_at->right, key);
            }
            //key is equal to the root's key
            else if (key == root_at->key){
                //there is only a right child here
                //so root will be replaced by the right child
                std::cout << "hereeee" << std::endl;
                if (root_at->left == nullptr){
                     std::cout << "hereLeft" << std::endl;
                    /*    x
                            x
                           x  x


                    */
                    BSTNode* temp = root_at->right;
                    //delete root_at;
                    std::cout << "hereLeft1" << std::endl;
                    //root = temp;
                    std::cout << "hereLeft2" << std::endl;
                    if (root_at == root){
                        root = temp;
                    }
                    return temp;
                }

                //there is only a left child here. root is replaced
                //by left child and original root deleted
                else if (root_at->right == nullptr){
                    std::cout << "hereright" << std::endl;
                    BSTNode* temp = root_at->left;
                    //delete root_at;
                    if (root_at == root){
                        root = temp;
                    }
                    return temp;
                }
                //root has both left and right children
                //so we replace the root's value with the right subtree's minimum
                //value
                else{
                    /*     x
                       x        x
                    x    x    Y   x

                    */
                    std::cout << "hereboth" << std::endl;
                    BSTNode* temp = findMinKeyNode(root_at->right);
                    root_at->key = temp->key;
                    root_at->value = temp->value;
                    
                    delete temp;
                    //root_at->right = remove(root_at->right, temp->key);
                }
            }
            return root_at;

        }

        
       
};

CoarseGrainedKeyValueStore_BST kvStore;
void concurrentInsertTest(CoarseGrainedKeyValueStore_BST& kvStore, int numThreads, int numIterations) {
    
    std::vector<std::thread> threads;

    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([&kvStore, i, numIterations]() {
            for (int j = 0; j < numIterations; ++j) {
                auto toInsert = "Value" + std::to_string(i * numIterations + j);
                //obj.valueMutex = 
                kvStore.insert(kvStore.root, i * numIterations + j, toInsert);
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

void concurrentRemoveTest(CoarseGrainedKeyValueStore_BST& kvStore, int numThreads, int numIterations) {
    
    std::vector<std::thread> threads;

    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([&kvStore, i, numIterations]() {
            for (int j = 0; j < numIterations; ++j) {
                //std::cout << "here in delete 1" << std::endl;
                kvStore.remove(kvStore.root, i * numIterations + j);
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

void concurrentUpdateTest(CoarseGrainedKeyValueStore_BST& kvStore, int numThreads, int numIterations) {
    
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
    CoarseGrainedKeyValueStore_BST kvStore;
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
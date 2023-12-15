#include <iostream>
#include <map>
#include <cassert>
#include <chrono>


class KeyValueStore {
    private:
        std::map<int, std::string> data; // Using int as key type for simplicity

    public:
        // Insertion function
        void insert(int key, const std::string& value) {
            auto it = data.find(key);
            if (it != data.end()) {
                return;
            } 
            data[key] = value;
        }

        void update(int key, const std::string& value){
            auto it = data.find(key);
            if (it != data.end()) {
                data[key] = value;
            } 
        }
        // Deletion function
        void remove(int key) {
            auto it = data.find(key);
            if (it != data.end()) {
                data.erase(it);
            } 
        }

        // Lookup function
        std::string lookup(int key) {
            auto it = data.find(key);
            if (it != data.end()) {
                return data[key];
                std::cout << "Key: " << key << ", Value: " << it->second << std::endl;
            } else {
                return "";
                std::cout << "Key not found: " << key << std::endl;
            }
        }
};

int sequential_insertion_tests(){
    KeyValueStore kvStore;

    kvStore.insert(1, "Value1");
    //make sure that it's properly inserted
    assert(kvStore.lookup(1) == "Value1");

    kvStore.insert(2, "Value2");
    //make sure that it's properly inserted
    assert(kvStore.lookup(2) == "Value2");

    //make sure that it returns "" when we look up something not there
    assert(kvStore.lookup(3) == ""); 

    kvStore.insert(3, "Value3");
    //make sure that it's properly inserted
    assert(kvStore.lookup(3) == "Value3");


    return 0;
}

//simulate threading but not really lol
int sequential_insert_tests_cmp_concurrent(KeyValueStore& kvStore, int numThreads, int numIterations, int k){
    auto start_time = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < numThreads; ++i) {
        for (int j = 0; j < numIterations; ++j) {
            auto toInsert = "Value" + std::to_string(i * numIterations + j);
                //obj.valueMutex = 
            kvStore.insert((i * numIterations + j) % k, toInsert);
        }
    }

    // End timing
    auto end_time = std::chrono::high_resolution_clock::now();

    // Calculate the duration in microseconds
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

    // Output the duration
    std::cout << "Seq k = " << k << " Insertion Map Time taken: " << duration.count() << " microseconds." << std::endl;
    // Verify that all values were inserted
    if (k >= numThreads * numIterations){
        for (int i = 0; i < numThreads * numIterations; ++i) {
            std::string result = kvStore.lookup(i);
            //std::cout << std::to_string(i) + " Result is " + result << std::endl;

            assert(result == "Value"+std::to_string(i)); // Assert that the value is not empty
        }
    }
    return 0;

}

//simulate threading but not really lol
int sequential_update_tests_cmp_concurrent(KeyValueStore& kvStore, int numThreads, int numIterations, int k){
    auto start_time = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < numThreads; ++i) {
        for (int j = 0; j < numIterations; ++j) {
            auto toInsert = "Value" + std::to_string((i * numIterations + j)*2);
            kvStore.update((i * numIterations + j) % k, toInsert);
        }
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    std::cout << "UPDATE k = " << k << " keys seq Map Time taken: " << duration.count() << " microseconds." << std::endl;

    if (k >= numThreads * numIterations){
    // Verify that all values were inserted
        for (int i = 0; i < numThreads * numIterations; ++i) {
            std::string result = kvStore.lookup(i);
            //std::cout << std::to_string(i) + " Result is " + result << std::endl;
            assert(result == "Value"+std::to_string(i*2)); // Assert that the value is not empty
        }
    }
    return 0;

}

//simulate threading but not really lol
int sequential_remove_tests_cmp_concurrent(KeyValueStore& kvStore, int numThreads, int numIterations, int k){
    auto start_time = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < numThreads; ++i) {
        for (int j = 0; j < numIterations; ++j) {
            
            kvStore.remove(i * numIterations + j);
        }
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    std::cout << "remove keys k = " << k << " seq Map Time taken: " << duration.count() << " microseconds." << std::endl;

    // Verify that all values were inserted
    if (k >= numThreads * numIterations){
        for (int i = 0; i < numThreads * numIterations; ++i) {
                std::string result = kvStore.lookup(i);
                //std::cout << std::to_string(i) + " Result is " + result << std::endl;
                assert(result == ""); // Assert that the value is not empty


        }
    }
    return 0;

}

//simulate threading but not really lol
int sequential_lookup_tests_cmp_concurrent(KeyValueStore& kvStore, int numThreads, int numIterations, int k){
    auto start_time = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < numThreads; ++i) {
        for (int j = 0; j < numIterations; ++j) {
            kvStore.lookup(i * numIterations + j);
        }
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    std::cout << "lookup keys k = " << k << " seq Map Time taken: " << duration.count() << " microseconds." << std::endl;


    return 0;

}



int tests_cmp_concurrent(){ 
    auto start_time = std::chrono::high_resolution_clock::now();
    int numThreads = 500;
    int numIters = 1000;
    KeyValueStore kvStore;

    for (int i = 1; i < numThreads * numIters * 10; i *= 10){
        sequential_insert_tests_cmp_concurrent(kvStore, numThreads, numIters, i);
        //std::cout << "Insertion tests cmp concurrent: All tests passed!" << std::endl;
    }

    for (int i = 1; i < numThreads * numIters * 10; i *= 10){
        sequential_update_tests_cmp_concurrent(kvStore, numThreads, numIters, i);
        //std::cout << "Update tests cmp concurrent: All tests passed!" << std::endl;
    }

    for (int i = 1; i < numThreads * numIters * 10; i *= 10){
        sequential_lookup_tests_cmp_concurrent(kvStore, numThreads, numIters, i);
        //std::cout << "Remove tests cmp concurrent: All tests passed!" << std::endl;
    }

    for (int i = 1; i < numThreads * numIters * 10; i *= 10){
        sequential_remove_tests_cmp_concurrent(kvStore, numThreads, numIters, i);
        //std::cout << "Remove tests cmp concurrent: All tests passed!" << std::endl;
    }



    // End timing
    auto end_time = std::chrono::high_resolution_clock::now();

    // Calculate the duration in microseconds
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

    // Output the duration
    std::cout << "Sequential Map Time taken: " << duration.count() << " microseconds." << std::endl;
    return 0;
}

int sequential_update_tests(){
    KeyValueStore kvStore;
    
    //first make sure that insert still works
    kvStore.insert(1, "Value1");
    //make sure that it's properly inserted
    assert(kvStore.lookup(1) == "Value1");

    //test updating an existing value
    //attempt to update value at key = 1
    kvStore.update(1, "Value1Updated");
    assert(kvStore.lookup(1) == "Value1Updated");

    //test updating a NON-existing value
    kvStore.update(2, "Value2Updated");
    assert(kvStore.lookup(2) == "");

    //first make sure that insert still works
    kvStore.insert(2, "Value2");
    //make sure that it's properly inserted
    assert(kvStore.lookup(2) == "Value2");

    //test updating an existing value
    kvStore.update(2, "Value2Updated");
    assert(kvStore.lookup(2) == "Value2Updated");

    //test that updating a key's value will ONLY affect that particular one
    assert(kvStore.lookup(1) == "Value1Updated");

    return 0;
}

int sequential_removal_tests(){
    KeyValueStore kvStore;
    
    //first make sure that insert still works
    kvStore.insert(1, "Value1");
    //make sure that it's properly inserted
    assert(kvStore.lookup(1) == "Value1");
    
    //test that basic removal of existing key-value works
    kvStore.remove(1);
    assert(kvStore.lookup(1) == "");

    //test that removal of NON-existing key-value works
    kvStore.remove(2);
    assert(kvStore.lookup(2) == "");

    //test that removal of an existing key-value WILL ONLY REMOVE THAT KEY-VALUE
    //first make sure that insert still works
    kvStore.insert(1, "Value1");
    //make sure that it's properly inserted
    assert(kvStore.lookup(1) == "Value1");

    //first make sure that insert still works
    kvStore.insert(2, "Value2");
    //make sure that it's properly inserted
    assert(kvStore.lookup(2) == "Value2");

    //test that removal of NON-existing key-value works
    kvStore.remove(2);
    assert(kvStore.lookup(2) == "");
    assert(kvStore.lookup(1) == "Value1");
    return 0;
}

int sequential_look_up_tests(){
    KeyValueStore kvStore;
    
    //look up non-existing key
    assert(kvStore.lookup(1) == "");

    //first make sure that insert still works
    kvStore.insert(1, "Value1");
    //make sure that it's properly inserted
    assert(kvStore.lookup(1) == "Value1");

    return 0;
}
int basic_sequential_tests(){
    sequential_look_up_tests();
    sequential_removal_tests();
    sequential_update_tests();
    sequential_insertion_tests();
    return 0;
}

int main() {
    basic_sequential_tests();
    std::cout << "Tests Sequential Passed" << std::endl;
    tests_cmp_concurrent();
    return 0;
}

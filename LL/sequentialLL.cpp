#include <iostream>
#include <map>
#include <cassert>

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
class KeyValueStore_LL {
    //OPTIMIZATION IDEA... WHEN SEARCHING MIGHT DO BINARY SEARCH INSTEAD OF LINEAR
    //SEARCH FOR SEARCHING THE VALUES LOLOLOLOL

    public:
        LinkedListNode* head;
        // Insertion function
        KeyValueStore_LL(){
            head = nullptr;
        }
        void insert(int key, const std::string& value) {
            //find key first
            LinkedListNode* pointer = lookup_ptr(key);
            if (pointer != nullptr){
                return;
            }

            //make new node
            LinkedListNode* toAdd = new LinkedListNode(key, value);
            toAdd->next = head;
            if (head) {//not null
                head->prev = toAdd;
            }
            head = toAdd;

        }

        void update(int key, const std::string& value){
            LinkedListNode* ptr = lookup_ptr(key);
            if (ptr == nullptr){
                return;
            }
            ptr->value = value;
        }

        // Lookup function
        std::string lookup(int key) {
            LinkedListNode* ptr = lookup_ptr(key);
            //std::cout << "here3" << std::endl;
            if (ptr == nullptr){
                return "";
            }
            return ptr->value;
        }

         // Lookup function
        LinkedListNode* lookup_ptr(int key) {
            LinkedListNode* pointer = head;
            while(pointer != nullptr){
                //std::cout << "here4" << std::endl;
                //std::cout << head->value << std::endl;
                
                if (pointer->key == key){
                    //std::cout << "exiting look up ptr w key" << std::endl;
                    return pointer;
                }
                //std::cout << "going to ptr next" << std::endl;
                pointer = pointer->next;
            }
            //std::cout << "exiting lookup ptr" << std::endl;
            return nullptr;
        }

        // Deletion function
        void remove(int key) {
            LinkedListNode* pointer = lookup_ptr(key);
            if (pointer == nullptr){
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

        }

       
};

int sequential_insertion_tests(){
    KeyValueStore_LL kvStore;

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
int sequential_update_tests(){
    KeyValueStore_LL kvStore;
    
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
    KeyValueStore_LL kvStore;
    
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
    KeyValueStore_LL kvStore;
    //std::cout << "here1" << std::endl;
    //look up non-existing key
    assert(kvStore.lookup(1) == "");
   // std::cout << "here2" << std::endl;

    //first make sure that insert still works
    kvStore.insert(1, "Value1");
    //make sure that it's properly inserted
    assert(kvStore.lookup(1) == "Value1");

    return 0;
}
int basic_sequential_tests(){
    sequential_look_up_tests();
    std::cout << "Tests lookup Passed" << std::endl;
    sequential_removal_tests();
    std::cout << "Tests removal Passed" << std::endl;
    sequential_update_tests();
    std::cout << "Tests update Passed" << std::endl;
    sequential_insertion_tests();
    std::cout << "Tests insertion Passed" << std::endl;
    return 0;
}
int main() {
    basic_sequential_tests();
    std::cout << "Tests Sequential Passed" << std::endl;
    return 0;
}
#include <iostream>
#include <map>
#include <cassert>

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
class KeyValueStore_BST{
    //OPTIMIZATION IDEA... WHEN SEARCHING MIGHT DO BINARY SEARCH INSTEAD OF LINEAR
    //SEARCH FOR SEARCHING THE VALUES LOLOLOLOL

    public:
        BSTNode* root;
        // Insertion function
        KeyValueStore_BST(){
            root = nullptr;
        }
        BSTNode* insert(BSTNode* root_at, int key, const std::string& value) {
            //find key first
             //no root
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
            BSTNode* ptr = lookup_ptr(root, key);
            if (ptr == nullptr){
                return;
            }
            ptr->value = value;
        }

        // Lookup function
        std::string lookup(int key) {
            BSTNode* ptr = lookup_ptr(root, key);
            //std::cout << "here3" << std::endl;
            if (ptr == nullptr){
                return "";
            }
            return ptr->value;
        }

         // Lookup function
        BSTNode* lookup_ptr(BSTNode* root, int key) {
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

int sequential_insertion_tests(){
    KeyValueStore_BST kvStore;

    kvStore.insert(kvStore.root, 1, "Value1");
    //make sure that it's properly inserted
    assert(kvStore.lookup(1) == "Value1");

    kvStore.insert(kvStore.root, 2, "Value2");
    //make sure that it's properly inserted
    assert(kvStore.lookup(2) == "Value2");

    //make sure that it returns "" when we look up something not there
    assert(kvStore.lookup(3) == ""); 

    kvStore.insert(kvStore.root, 3, "Value3");
    //make sure that it's properly inserted
    assert(kvStore.lookup(3) == "Value3");


    return 0;
}
int sequential_update_tests(){
    KeyValueStore_BST kvStore;
    
    //first make sure that insert still works
    kvStore.insert(kvStore.root, 1, "Value1");
    //make sure that it's properly inserted
    assert(kvStore.lookup(1) == "Value1");

    //test updating an existing value
    //attempt to update value at key = 1
    kvStore.update( 1, "Value1Updated");
    assert(kvStore.lookup(1) == "Value1Updated");

    //test updating a NON-existing value
    kvStore.update(2, "Value2Updated");
    assert(kvStore.lookup(2) == "");

    //first make sure that insert still works
    kvStore.insert(kvStore.root, 2, "Value2");
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
    KeyValueStore_BST kvStore;
    
    //first make sure that insert still works
    kvStore.insert(kvStore.root, 1, "Value1");
    //make sure that it's properly inserted
    assert(kvStore.lookup(1) == "Value1");
    
    //test that basic removal of existing key-value works
    kvStore.remove(kvStore.root, 1);
    assert(kvStore.lookup(1) == "");

    //test that removal of NON-existing key-value works
    kvStore.remove(kvStore.root, 2);
    assert(kvStore.lookup(2) == "");

    //test that removal of an existing key-value WILL ONLY REMOVE THAT KEY-VALUE
    //first make sure that insert still works
    kvStore.insert(kvStore.root, 1, "Value1");
    //make sure that it's properly inserted
    assert(kvStore.lookup(1) == "Value1");

    //first make sure that insert still works
    kvStore.insert(kvStore.root, 2, "Value2");
    //make sure that it's properly inserted
    assert(kvStore.lookup(2) == "Value2");

    //test that removal of NON-existing key-value works
    kvStore.remove(kvStore.root, 2);
    assert(kvStore.lookup(2) == "");
    assert(kvStore.lookup(1) == "Value1");
    return 0;
}

int sequential_look_up_tests(){
    KeyValueStore_BST kvStore;
    //std::cout << "here1" << std::endl;
    //look up non-existing key
    assert(kvStore.lookup(1) == "");
   // std::cout << "here2" << std::endl;

    //first make sure that insert still works
    kvStore.insert(kvStore.root, 1, "Value1");
    //make sure that it's properly inserted
    assert(kvStore.lookup(1) == "Value1"); //change

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

int main(){
    basic_sequential_tests();
    std::cout << "Tests Sequential Passed" << std::endl;
    return 0;
}

#include <iostream>
#include <map>
#include <cassert>
#include <thread>
#include <mutex>
#include <vector>
#include <unordered_map>
#include <string.h>
#include "timing.h"
#include "mpi.h"

#define CAPACITY 5
#define MAXLEN 120
#define BUCKET_SIZE 2

struct Node {
  int key;
  std::string value;
  int valid;
};

void LockFreeKeyValueStoreMPI(std::vector<std::vector<Node>> &map) {
    for (int i = 0; i < CAPACITY; i++){
        std::vector<Node> it;
        for (int j = 0; j < BUCKET_SIZE; j++) {
            Node node;
            node.key = -1;
            node.value= "";
            node.valid = -1;
            it.push_back(node);
        }
        map.push_back(it);
    }
}

// Insertion function
void insert(std::vector<std::vector<Node>> &map, int key, const std::string& value) {
    auto nodes = map[key%CAPACITY];
    for (int i=0; i < nodes.size(); i++){
        Node it = nodes[i];
        printf("looking at key1 %d\n", it.key);
        if (it.valid == -1){
            printf("found key %d\n", it.key);
            it.key = key;
            it.value = value;
            it.valid = 1;
            nodes[i] = it;
            map[key%CAPACITY] = nodes;
            printf("works key %d\n", map[key%CAPACITY][i].key);
            return;
        }
    }
}

void update(std::vector<std::vector<Node>> &map, int key, const std::string& value){
    auto nodes = map[key%CAPACITY];
    for (auto &it: nodes){
        if (it.key == key && it.valid == 1){
            it.value = value;
            return;
        }
    }
}
// Deletion function
void remove(std::vector<std::vector<Node>> &map, int key) {
    auto nodes = map[key%CAPACITY];
    for (auto &it: nodes){
        if (it.key == key && it.valid == 1){
            it.valid = 0;
            return;
        }
    }
}

// Lookup function
void lookup(std::vector<std::vector<Node>> &map, int key, std::string& val) {
  printf("looking for key %d with mod %d\n", key, key%CAPACITY);
    auto nodes = map[key%CAPACITY];
    for (auto &it: nodes){
        printf("looking at key %d\n", it.key);
        if (it.key == key && it.valid == 1){
            val = it.value;
            return;
        }
    }
}

int main(int argc, char *argv[]) {
  int pid;
  int nproc;
  std::vector<std::vector<Node>> map;

  int numNodes = 10;
  int nodesPerBucket = numNodes/CAPACITY;

  // Initialize MPI
  MPI_Init(&argc, &argv);
  // Get process rank
  MPI_Comm_rank(MPI_COMM_WORLD, &pid);
  // Get total number of processes specificed at start of run
  MPI_Comm_size(MPI_COMM_WORLD, &nproc);
  
  if (pid == 0){
    LockFreeKeyValueStoreMPI(map);
    printf("map size %ld\n", map[0].size());
  } else {
    map.resize(CAPACITY);
    for (int i=0; i < CAPACITY; i++){
        std::vector<Node> it;
        it.resize(BUCKET_SIZE);
        map[i] = it;
    }
  }

  for (int i=0; i < CAPACITY; i++){
        MPI_Bcast(map[i].data(), BUCKET_SIZE * sizeof(Node), MPI_BYTE, 0, MPI_COMM_WORLD);
    }

  

  //std::vector<LinkedListNode*> map;
  //map.resize(CAPACITY);
  // Don't change the timeing for totalSimulationTime.
  int displs[nproc];
  int recvcounts[nproc];
  int batchSize = CAPACITY/nproc;
  int batchRemainder = CAPACITY % nproc;

  printf("size %d, rem %d\n",batchSize, batchRemainder);

  for (int i = 0; i < batchRemainder; i++){
    displs[i] = i * (batchSize + 1) * BUCKET_SIZE * sizeof(Node);
    recvcounts[i] = (batchSize + 1) * BUCKET_SIZE * sizeof(Node);
  }
  for (int i = batchRemainder; i < nproc; i++){
    displs[i] = (batchRemainder * (batchSize + 1) + (i - batchRemainder) * batchSize) * BUCKET_SIZE * sizeof(Node);
    recvcounts[i] = batchSize * BUCKET_SIZE * sizeof(Node);
  }
  //printf("displ0 %d, displ1%d, size%d", displs[0], displs[1], displs)
  int disp = displs[pid]/(BUCKET_SIZE * sizeof(Node));
  int count = recvcounts[pid]/(BUCKET_SIZE * sizeof(Node));

  // for (int i = disp; i < disp+count; i++){
  //       std::vector<Node> it;
  //       for (int j = 0; j < BUCKET_SIZE; j++) {
  //           Node node;
  //           node.key = -1;
  //           node.value= "";
  //           node.valid = -1;
  //           it.push_back(node);
  //       }
  //       map[i] = it;
  //   }

  printf("disp %d count %d for pid %d\n", disp, count, pid);
  MPI_Barrier(MPI_COMM_WORLD);
  Timer totalSimulationTimer;
  //auto kvstore = LockFreeKeyValueStoreMPI(count);
  for (int i = disp; i < disp+count; i++) {
    for(int j = 0; j < nodesPerBucket; j++){
        printf("DOING FIRST INSERT for pid %d\n", pid);
        auto toInsert = "Value" + std::to_string(j*CAPACITY + i);
        insert(map, j*CAPACITY + i, toInsert);
    }
  }

  for (auto &nodes: map) {
    for (auto &n: nodes){
      //printf("GOT key %d\n", n.key);
    }
  }
  MPI_Barrier(MPI_COMM_WORLD);

  std::vector<Node> temp;
  for (auto &n: map[disp]){
      printf("GOT key12 %d\n", n.key);
      temp.push_back(n);
    }
  // MPI_Gather(temp.data(), BUCKET_SIZE * sizeof(Node), MPI_BYTE, map[disp].data(), 
  //      BUCKET_SIZE * sizeof(Node), MPI_BYTE, 0, MPI_COMM_WORLD);

  for (int i=0; i < count; i++){
    printf("Broadcasting %d for pid %d\n", disp+i, pid);
    std::vector<Node> temp;
    for (auto &n: map[disp+i]){
      printf("GOT key12 %d\n", n.key);
      temp.push_back(n);
    }
    MPI_Bcast(map[disp].data(), BUCKET_SIZE * sizeof(Node), MPI_BYTE, pid, MPI_COMM_WORLD);
  }
  MPI_Barrier(MPI_COMM_WORLD);
  // for (int i=0; i < count; i++){
  //       MPI_Bcast(map[disp+i].data(), BUCKET_SIZE * sizeof(Node), MPI_BYTE, pid, MPI_COMM_WORLD);
  //   }

//   MPI_Allgatherv(map[disp].data(), count*BUCKET_SIZE * sizeof(Node), MPI_BYTE, map.data(), 
//       recvcounts, displs, MPI_BYTE, MPI_COMM_WORLD);

  if (pid == 0) {
    for (auto &nodes: map) {
      for (auto &n: nodes){
        printf("GOT key %d\n", n.key);
      }
    }
  }
  double totalSimulationTime = totalSimulationTimer.elapsed();

//   for (auto elem: kvstoreMain.map){
//     printf("got key %d\n", elem->key);
//   }

  if(pid == 0) {
    for (int i = 0; i < numNodes; ++i) {
        std::string str_ptr = "";
        lookup(map, i, str_ptr);
        //if (str_ptr != "Value"+std::to_string(i)){
        std::cout << std::to_string(i) + " Result is " + str_ptr << std::endl;
        //}
        assert(str_ptr == "Value"+std::to_string(i)); // Assert that the value is not empty
    }
    std::cout << "CONCURRENT INSERT: All tests passed!" << std::endl;
  }

  MPI_Finalize();
}
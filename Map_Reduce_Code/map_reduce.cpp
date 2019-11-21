#include <stdio.h>
#include <stdlib.h>
#include <omp.h>
#include <mpi.h>
#include <ctype.h>
#include <dirent.h>
#include <stddef.h>
#include <unistd.h>
#include <map>
#include <sstream>
#include <algorithm>
#include <vector>
#include <string>
#include <cstring>
#include <fstream>
#include <iostream>

using namespace std;

//Size of a work queue

#define WORKQ_SIZE 10000

//Reader thread, mapper thread and reducer thread counts

#define READER_THREADS 1
#define MAPPER_THREADS 1
#define REDUCER_THREADS 8
#define SENDER_THREADS 8
#define RECEIVER_THREADS 8
#define MASTER_THREAD 1

//Other very important constants 

#define MAX_FILENAME_LENGTH 30
#define LOOP_OVER_DIRECTORY 8
#define NUMBER_OF_BINS 1024
#define FILE_BROADCAST_TAG 99
#define FILE_REQUEST_TAG 88
#define VECTOR_BROADCAST_TAG 22
#define VECTOR_SIZE_BROADCAST_TAG 33

//Constants needed for time keeping
#define OVERALL 0
#define READERS 1
#define MAPPERS 2
#define SENDERS 3
#define RECEIVERS 4
#define OUTPUTFILE 5

typedef vector<string> vector_string;
typedef map<string, long> map_tuple;
typedef pair<string, long> pair_tuple;
typedef vector<map_tuple> vector_map;

//The tuple that contains the word and it's count
class tuple_item {
public:
  char word[40];
  long count;
  int read_unread;    //read = 1, unread = 0 (Has the data been read or is it still unread)

  tuple_item() {
    count = 0;
    read_unread = 0;
  }
};

typedef vector<tuple_item> vector_tuple;

//Struct for the work queue
class workQ {
public:
  int read_ptr;
  int write_ptr;
  tuple_item dataQ[WORKQ_SIZE];
  
  //Constructor and Functions

  workQ() {
    read_ptr = 0;
    write_ptr = 0;
    for(int i = 0; i < WORKQ_SIZE; i++)
      dataQ[i].read_unread = 1;
  }
  int Qempty_check() {
    return (this->read_ptr == this->write_ptr && this->dataQ[this->write_ptr].read_unread);
  }
  
  int Qfull_check() {
    return (this->read_ptr == this->write_ptr && !this->dataQ[this->read_ptr].read_unread);
  }

  int read_fromQ(tuple_item * return_data) {
    if(this->Qempty_check())
      return 0;   //tried to read from an empty work queue
    
    this->dataQ[this->read_ptr].read_unread = 1;
    *return_data = this->dataQ[this->read_ptr];
    this->read_ptr = (this->read_ptr + 1) % WORKQ_SIZE;

    return 1;
  }

  int write_toQ(tuple_item incoming_data) {
    if(this->Qfull_check())
      return 0;   //tried to write to a full work queue

    this->dataQ[this->write_ptr] = incoming_data;
    this->dataQ[this->write_ptr].read_unread = 0;
    this->write_ptr = (this->write_ptr + 1) % WORKQ_SIZE;

    return 1;
  }
};

//Queues for all the threads

workQ reader_mapperQ[READER_THREADS];

//Locks for all the threads

omp_lock_t reader_mapper_locks[READER_THREADS];

double total_critical_time;

void BroadcastFileNames (vector_string file_names, int num_p) {

  vector_string::iterator index;
  for(int i = 0; i < LOOP_OVER_DIRECTORY; i++) {
    for(index = file_names.begin(); index != file_names.end(); index++) {
      MPI_Status status;
      int flag;
      int message_length = (*index).size() + 1;
      MPI_Recv(&flag, 1, MPI_INT, MPI_ANY_SOURCE, FILE_REQUEST_TAG, MPI_COMM_WORLD, &status);
      MPI_Send((*index).c_str(), message_length, MPI_CHAR, status.MPI_SOURCE, FILE_BROADCAST_TAG, MPI_COMM_WORLD);
      //printf("\nSent %s to thread %d in process %d\n", (*index).c_str(), flag, status.MPI_SOURCE);
    }
  }
  //printf("\nDebug Message: File name sender finished sending the file names to all of the threads in all the processes. Need to send complete messages now.\n");
  for(int i = 0; i < READER_THREADS * num_p; i++) {
    MPI_Status status;
    int flag;
    char complete_message[25] = "CompletedReadingAllFiles";
    MPI_Recv(&flag, 1, MPI_INT, MPI_ANY_SOURCE, FILE_REQUEST_TAG, MPI_COMM_WORLD, &status);
    MPI_Send(complete_message, MAX_FILENAME_LENGTH, MPI_CHAR, status.MPI_SOURCE, FILE_BROADCAST_TAG, MPI_COMM_WORLD);
    //printf("\nSent %s to thread %d in process %d\n", complete_message, flag, status.MPI_SOURCE);
  }
  //printf("\nDebug Message: File name sender finished sending the complete message to all of the threads in all the processes.\n");
}

void WorkOfReaders(int pid, int tid, string file_name) {

  ifstream file_to_read;
  file_to_read.open(file_name);
  if(file_to_read.is_open()) {
    string incoming_word;
    int reader_mapper_workQ = tid % READER_THREADS;
    while(file_to_read >> incoming_word) {
      for(int i = 0, word_len = incoming_word.size(); i < word_len; i++) {
	if(ispunct(incoming_word[i])) {
	  incoming_word.erase(i--, 1);
	  word_len = incoming_word.size();
	}
      }
      if(incoming_word == "") continue;
      transform(incoming_word.begin(), incoming_word.end(), incoming_word.begin(), ::tolower);
      tuple_item outgoing_word;
      strcpy(outgoing_word.word, incoming_word.c_str());
      outgoing_word.count = 1;
      outgoing_word.read_unread = 0;
      int atomicOpSuccess = 0;
      while(!atomicOpSuccess) {
	omp_set_lock(&reader_mapper_locks[reader_mapper_workQ]);
	atomicOpSuccess = reader_mapperQ[reader_mapper_workQ].write_toQ(outgoing_word);
	omp_unset_lock(&reader_mapper_locks[reader_mapper_workQ]);
	if(!atomicOpSuccess) {
	  //printf("\nProcess %d's, reader thread %d failed to write to it's Reader-Mapper Queue.\n",pid, tid);
	  usleep(2000);
	}
      }
    }
  }
  else
    printf("\nFailed to open the file. File path: %s\n", file_name.c_str());
  file_to_read.close();
  //printf("\nThread %d in process %d finished reading file %s\n", tid, pid, file_name.c_str());
  return;
}

void WorkOfMappers(int pid, vector_map &local_hashmap) {
  
#pragma omp parallel num_threads(MAPPER_THREADS)
  {
    int allFilesCompleteFlag = 0;
    int tid = omp_get_thread_num();
    int reader_mapper_workQ = tid % MAPPER_THREADS;
    tuple_item incoming_tuple;
    double critical_start_time, critical_end_time;
    while(!allFilesCompleteFlag) {
      int atomicOpSuccess = 0;
      while(!atomicOpSuccess) {
	omp_set_lock(&reader_mapper_locks[reader_mapper_workQ]);
	atomicOpSuccess = reader_mapperQ[reader_mapper_workQ].read_fromQ(&incoming_tuple);
	omp_unset_lock(&reader_mapper_locks[reader_mapper_workQ]);
	if(!atomicOpSuccess) {
	  //printf("\nProcess %d's, mapper thread %d failed to read from it's Reader-Mapper Queue.\n",pid, tid);
	  usleep(2000);
	}
      }
      if(!strcmp(incoming_tuple.word, "CompletedReadingAllFiles"))
	allFilesCompleteFlag = 1;
      else {
	unsigned long int hashmap_val = 0;
	for(int i = 0; i < strlen(incoming_tuple.word); i++)
	  hashmap_val += 22 + 23 * (incoming_tuple.word[i] - 14); //Trying to make the hash function as complicated as possible to achieve even bucket sizes
	int hashmap_bin = hashmap_val % NUMBER_OF_BINS;
	map_tuple::iterator index = local_hashmap[hashmap_bin].find(incoming_tuple.word);
	#pragma omp critical
	{
	  critical_start_time = omp_get_wtime();
	  if(index == local_hashmap[hashmap_bin].end()) {
	      local_hashmap[hashmap_bin].insert(pair_tuple(incoming_tuple.word, incoming_tuple.count));
	  }
	  else {
	    //map_tuple::iterator index = local_hashmap[hashmap_bin].find(incoming_tuple.word);
	    (*index).second += incoming_tuple.count;
	  }
	  critical_end_time = omp_get_wtime();
	  total_critical_time += critical_end_time - critical_start_time;
	}
      }
    }
    //printf("\nMapper thread %d of process %d finished mapping it's queue into the hashmap.\n", tid, pid);
  }
}

void WorkOfSendersSequential(int pid, int num_p, vector_map &incoming_hashmap) {

  int count = 3;
  int blockLengths[3] = {sizeof(char) * 40, sizeof(long), sizeof(int)};
  MPI_Aint displArray[3] = {offsetof(tuple_item, word), offsetof(tuple_item, count), offsetof(tuple_item, read_unread)};
  MPI_Datatype typeArray[3] = {MPI_CHAR, MPI_LONG, MPI_INT};
  MPI_Datatype temp_tupletype, tuple_type;
  MPI_Aint lower_bound, extent;
  MPI_Type_create_struct(count, blockLengths, displArray, typeArray, &temp_tupletype);
  MPI_Type_get_extent(temp_tupletype, &lower_bound, &extent);
  MPI_Type_create_resized(temp_tupletype, lower_bound, extent, &tuple_type);
  MPI_Type_commit(&tuple_type);
  int threads_to_make = (num_p == 1)? 1 : num_p - 1;
  
  //#pragma omp parallel num_threads(threads_to_make)
  //{
    int tid = omp_get_thread_num();
    int dest = (num_p == 1)? tid : ((tid < pid)? tid : tid + 1);
    vector_tuple vector_to_send;
    tuple_item new_word;
    int p = (pid + 1) % num_p;
    //printf("\nProcess %d's starting process to send to: %d\n", pid, p);
    for(int counter = 0; counter < num_p; counter++) {
      if(p != pid) {
	for(int i = p; i < NUMBER_OF_BINS; i += num_p) {
	  map_tuple::iterator index;
	  for(index = incoming_hashmap[i].begin(); index != incoming_hashmap[i].end(); index++) {
	    strcpy(new_word.word, (*index).first.c_str());
	    new_word.count = (*index).second;
	    vector_to_send.push_back(new_word);
	  }
	}
	int size_of_vector = vector_to_send.size();
    
	//printf("\nProcess %d preparing to send a hashmap vector to process %d.\n", pid, p);
    
	MPI_Send(&size_of_vector, 1, MPI_INT, p, VECTOR_SIZE_BROADCAST_TAG, MPI_COMM_WORLD);
	usleep(2000);
	for(int i = 0; i < size_of_vector; i++)
	  MPI_Send(&vector_to_send[i], 1, tuple_type, p, VECTOR_BROADCAST_TAG, MPI_COMM_WORLD);
      }
      p = (p + 1) % num_p;
      vector_to_send.clear();
      //}
      //printf("\nProcess %d sent it's hashmap vector to process %d.\n", pid, p);
    }
}

void WorkOfSenders(int pid, int num_p, vector_map &incoming_hashmap) {

  int count = 3;
  int blockLengths[3] = {sizeof(char) * 40, sizeof(long), sizeof(int)};
  MPI_Aint displArray[3] = {offsetof(tuple_item, word), offsetof(tuple_item, count), offsetof(tuple_item, read_unread)};
  MPI_Datatype typeArray[3] = {MPI_CHAR, MPI_LONG, MPI_INT};
  MPI_Datatype temp_tupletype, tuple_type;
  MPI_Aint lower_bound, extent;
  MPI_Type_create_struct(count, blockLengths, displArray, typeArray, &temp_tupletype);
  MPI_Type_get_extent(temp_tupletype, &lower_bound, &extent);
  MPI_Type_create_resized(temp_tupletype, lower_bound, extent, &tuple_type);
  MPI_Type_commit(&tuple_type);
  int threads_to_make = (num_p == 1)? 1 : num_p - 1;
  
#pragma omp parallel num_threads(threads_to_make)
  {
    int tid = omp_get_thread_num();
    int dest = (num_p == 1)? tid : ((tid < pid)? tid : tid + 1);
    vector_tuple vector_to_send;
    tuple_item new_word;
    for(int i = dest; i < NUMBER_OF_BINS; i += num_p) {
      map_tuple::iterator index;
      for(index = incoming_hashmap[i].begin(); index != incoming_hashmap[i].end(); index++) {
	strcpy(new_word.word, (*index).first.c_str());
	new_word.count = (*index).second;
	vector_to_send.push_back(new_word);
      }
    }
    int size_of_vector = vector_to_send.size();
    //printf("\nThread %d of process %d preparing to send a hashmap vector to process %d.\n", tid, pid, dest);
    MPI_Send(&size_of_vector, 1, MPI_INT, dest, VECTOR_SIZE_BROADCAST_TAG, MPI_COMM_WORLD);
    usleep(2000);
    for(int i = 0; i < size_of_vector; i++)
      MPI_Send(&vector_to_send[i], 1, tuple_type, dest, VECTOR_BROADCAST_TAG, MPI_COMM_WORLD);
    //printf("\nThread %d of process %d sent it's hashmap vector to process %d.\n", tid, pid, dest);
  }
}

void WorkOfReceiversSequential(int pid, int num_p, vector_map &incoming_hashmap, map_tuple &output_hashmap) {

  int count = 3;
  int blockLengths[3] = {sizeof(char) * 40, sizeof(long), sizeof(int)};
  MPI_Aint displArray[3] = {offsetof(tuple_item, word), offsetof(tuple_item, count), offsetof(tuple_item, read_unread)};
  MPI_Datatype typeArray[3] = {MPI_CHAR, MPI_LONG, MPI_INT};
  MPI_Datatype temp_tupletype, tuple_type;
  MPI_Aint lower_bound, extent;
  MPI_Type_create_struct(count, blockLengths, displArray, typeArray, &temp_tupletype);
  MPI_Type_get_extent(temp_tupletype, &lower_bound, &extent);
  MPI_Type_create_resized(temp_tupletype, lower_bound, extent, &tuple_type);
  MPI_Type_commit(&tuple_type);

  tuple_item new_word;
  for(int i = pid; i < NUMBER_OF_BINS; i += num_p) {
    map_tuple::iterator index;
    for(index = incoming_hashmap[i].begin(); index != incoming_hashmap[i].end(); index++) {
      output_hashmap.insert(pair_tuple((*index).first, (*index).second));
    }
  }

  int threads_to_make = (num_p == 1)? 1 : num_p - 1;
  
  //#pragma omp parallel num_threads(threads_to_make)
  //{
    vector_tuple vector_to_recv;
    tuple_item new_item;
    int size;
    int tid = omp_get_thread_num();
    int src = (num_p == 1)? tid : ((tid < pid)? tid : tid + 1);
    MPI_Status status;
    int p = (pid + num_p - 1) % num_p;
    //printf("\nProcess %d's starting process to receive from: %d\n", pid, p);
    
    for(int counter = 0; counter < num_p; counter++) {
      //printf("\nProcess %d preparing to receive a hashmap vector from process %d.\n", pid, p);
      if(p != pid) {
	MPI_Recv(&size, 1, MPI_INT, p, VECTOR_SIZE_BROADCAST_TAG, MPI_COMM_WORLD, &status);
	//usleep(2000);
	for(int i = 0; i < size; i++) {
	  MPI_Recv(&new_item, 1, tuple_type, p, VECTOR_BROADCAST_TAG, MPI_COMM_WORLD, &status);
	  vector_to_recv.push_back(new_item);
	}
      }
      p = (p + num_p - 1) % num_p;
      //printf("\nProcess %d received a hashmap vector from process %d.\n", pid, p);

      map_tuple::iterator index;
      for(int i = 0; i < vector_to_recv.size(); i++) {
	index = output_hashmap.find(vector_to_recv[i].word);
      //#pragma omp critical
      //{
	if(index == output_hashmap.end())
	  output_hashmap.insert(pair_tuple(vector_to_recv[i].word, vector_to_recv[i].count));
	else
	  (*index).second += vector_to_recv[i].count;
	//}
      }
      vector_to_recv.clear();
    }
}

void WorkOfReceivers(int pid, int num_p, vector_map &incoming_hashmap, map_tuple &output_hashmap) {

  int count = 3;
  int blockLengths[3] = {sizeof(char) * 40, sizeof(long), sizeof(int)};
  MPI_Aint displArray[3] = {offsetof(tuple_item, word), offsetof(tuple_item, count), offsetof(tuple_item, read_unread)};
  MPI_Datatype typeArray[3] = {MPI_CHAR, MPI_LONG, MPI_INT};
  MPI_Datatype temp_tupletype, tuple_type;
  MPI_Aint lower_bound, extent;
  MPI_Type_create_struct(count, blockLengths, displArray, typeArray, &temp_tupletype);
  MPI_Type_get_extent(temp_tupletype, &lower_bound, &extent);
  MPI_Type_create_resized(temp_tupletype, lower_bound, extent, &tuple_type);
  MPI_Type_commit(&tuple_type);

  tuple_item new_word;
  for(int i = pid; i < NUMBER_OF_BINS; i += num_p) {
    map_tuple::iterator index;
    for(index = incoming_hashmap[i].begin(); index != incoming_hashmap[i].end(); index++) {
      output_hashmap.insert(pair_tuple((*index).first, (*index).second));
    }
  }

  int threads_to_make = (num_p == 1)? 1 : num_p - 1;
  
#pragma omp parallel num_threads(threads_to_make)
  {
    vector_tuple vector_to_recv;
    tuple_item new_item;
    int size;
    int tid = omp_get_thread_num();
    int src = (num_p == 1)? tid : ((tid < pid)? tid : tid + 1);
    MPI_Status status;
    //printf("\nThread %d of process %d preparing to receive a hashmap vector from process %d.\n", tid, pid, src);
    MPI_Recv(&size, 1, MPI_INT, src, VECTOR_SIZE_BROADCAST_TAG, MPI_COMM_WORLD, &status);
    //usleep(2000);
    for(int i = 0; i < size; i++) {
      MPI_Recv(&new_item, 1, tuple_type, src, VECTOR_BROADCAST_TAG, MPI_COMM_WORLD, &status);
      vector_to_recv.push_back(new_item);
    }
    //printf("\nThread %d of process %d received a hashmap vector from process %d.\n", tid, pid, src);

    map_tuple::iterator index;
    for(int i = 0; i < vector_to_recv.size(); i++) {
      index = output_hashmap.find(vector_to_recv[i].word);
#pragma omp critical
      {
	if(index == output_hashmap.end())
	  output_hashmap.insert(pair_tuple(vector_to_recv[i].word, vector_to_recv[i].count));
	else
	  (*index).second += vector_to_recv[i].count;
      }
    }
  }
}

void OutputToFile(int pid, map_tuple &output_hashmap) {
  ofstream resultFile;
  stringstream pid_string;
  pid_string << pid;
  string outputFileName = "Process_" + pid_string.str() + "_Output_File.txt";
  resultFile.open(outputFileName.c_str());

  for(map_tuple::iterator index = output_hashmap.begin(); index != output_hashmap.end(); index++)
    resultFile << "<" << (*index).first << ", " << (*index).second << "> \n";
  resultFile.close();
}

int main(int argc, char* argv[]) {
  int num_p, pid, provided;

  //Variables needed for keeping track of time;
  double start_time[6], end_time[6], local_time[7], final_time[7];

  vector_string allFileNames;
  map_tuple output_hashmap;
  vector_map local_hashmap;
  string file_name;
  
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  MPI_Comm_size(MPI_COMM_WORLD, &num_p);
  MPI_Comm_rank(MPI_COMM_WORLD, &pid);

  for(int i = 0; i < MAPPER_THREADS; i++)
    omp_init_lock(&reader_mapper_locks[i]);
  
  for(int i = 0; i < NUMBER_OF_BINS; i++)
    local_hashmap.push_back(map_tuple());
  
  omp_set_nested(1);
  //omp_set_num_threads(MASTER_THREAD + READER_THREADS + MAPPER_THREADS + SENDER_THREADS + RECEIVER_THREADS);
  omp_set_num_threads(omp_get_max_threads() + MASTER_THREAD);
  
  if(!pid) {
    DIR* file_directory;
    struct dirent *dir_iterator;
    file_directory = opendir("./RawText/");
    if(file_directory == NULL) {
      printf("\nCouldn't open the directory!\n");
      return 0;
    }
    while(dir_iterator = readdir(file_directory)) {
      if(strcmp(dir_iterator->d_name,".") != 0 && strcmp(dir_iterator->d_name,"..") != 0) {
	stringstream pullOutFile;
	pullOutFile << dir_iterator->d_name;
	pullOutFile >> file_name;
	file_name = "./RawText/" + file_name;
	allFileNames.push_back(file_name);
      }
    }
    closedir(file_directory);
  }
  
  start_time[OVERALL] = MPI_Wtime();

#pragma omp parallel num_threads(3)
  {
    int tid = omp_get_thread_num();

    //Global master thread that is sending the file names to all the reader threads in all the processes.
#pragma omp master
    {
      if(!pid)
	BroadcastFileNames(allFileNames, num_p);
    }
	
    //Master Reader Thread for each Processor. This needs to work concurrently with the master mapper thread so using a nowait clause at the end of the pragma omp single.
#pragma omp single nowait
    {
      start_time[READERS] = MPI_Wtime();

#pragma omp parallel num_threads(READER_THREADS)
      {
	char incoming_file[30];
	int tid = omp_get_thread_num();
	int reader_mapper_workQ = tid % READER_THREADS;
	do {
	  int flag = tid;
	  MPI_Status status;
	  MPI_Send(&flag, 1, MPI_INT, 0, FILE_REQUEST_TAG, MPI_COMM_WORLD);
	  //printf("\nThread %d in process %d requesting a file.\n", tid, pid);
	  MPI_Recv(&incoming_file, MAX_FILENAME_LENGTH, MPI_CHAR, 0, FILE_BROADCAST_TAG, MPI_COMM_WORLD, &status);
	  //printf("\nThread %d in process %d received file %s from thread 0 in process 0.\n", tid, pid, incoming_file);
	  if(strcmp(incoming_file, "CompletedReadingAllFiles"))
	    WorkOfReaders(pid, tid, incoming_file);
	} while(strcmp(incoming_file, "CompletedReadingAllFiles"));
	
	if(!strcmp(incoming_file, "CompletedReadingAllFiles")) {
	  tuple_item finished_reading;
	  strcpy(finished_reading.word, incoming_file);
	  finished_reading.count = -2;
	  finished_reading.read_unread = 0;
	  int atomicOpSuccess = 0;
	  while(!atomicOpSuccess) {
	    omp_set_lock(&reader_mapper_locks[reader_mapper_workQ]);
	    atomicOpSuccess = reader_mapperQ[reader_mapper_workQ].write_toQ(finished_reading);
	    omp_unset_lock(&reader_mapper_locks[reader_mapper_workQ]);
	    if(!atomicOpSuccess) {
	      //printf("\nProcess %d's, reader thread %d failed to write to it's Reader-Mapper Queue.\n",pid, tid);
	      usleep(2000);
	    }
	  }
	}
	//printf("\nThread %d of process %d finished reading all the files sent to it from the master process.\n", tid, pid);
      }
      end_time[READERS] = MPI_Wtime();
      local_time[READERS] = end_time[READERS] - start_time[READERS];
    }
  
  //Master Mapper Thread for each Processor. Using a pragma omp single to take advantage of the implicit barrier placed at the end of it.
#pragma omp single
    {
      start_time[MAPPERS] = MPI_Wtime();

      WorkOfMappers(pid, local_hashmap);

      end_time[MAPPERS] = MPI_Wtime();
      local_time[MAPPERS] = end_time[MAPPERS] - start_time[MAPPERS];
    }
  
    //Master Sender Thread for each Processor. This needs to work concurrently with the receiver thread so using a nowait clause at the end of pragma omp single.
#pragma omp single nowait
    {
      start_time[SENDERS] = MPI_Wtime();
      
      //WorkOfSendersSequential(pid, num_p, local_hashmap);
      WorkOfSenders(pid, num_p, local_hashmap);

      end_time[SENDERS] = MPI_Wtime();
      local_time[SENDERS] = end_time[SENDERS] - start_time[SENDERS];
    }
    
    //Master Receiver Thread for each processor. Using multiple threads to receive the vectors from the other processes
    //concurrently. But need a critical section since you need to apply changes to the local map of words sequentially.
#pragma omp single
    {
      start_time[RECEIVERS] = MPI_Wtime();

      //WorkOfReceiversSequential(pid, num_p, local_hashmap, output_hashmap);
      WorkOfReceivers(pid, num_p, local_hashmap, output_hashmap);

      end_time[RECEIVERS] = MPI_Wtime();
      local_time[RECEIVERS] = end_time[RECEIVERS] - start_time[RECEIVERS];
    }
  }
  
  start_time[OUTPUTFILE] = MPI_Wtime();
  
  OutputToFile(pid, output_hashmap);

  end_time[OUTPUTFILE] = MPI_Wtime();
  local_time[OUTPUTFILE] = end_time[OUTPUTFILE] - start_time[OUTPUTFILE];
  
  //printf("\nProcess %d has finished outputting it's hashmap into a file.\n", pid);

  end_time[OVERALL] = MPI_Wtime();
  
  local_time[OVERALL] = end_time[OVERALL] - start_time[OVERALL];
  local_time[6] = total_critical_time;

  MPI_Barrier(MPI_COMM_WORLD);

  MPI_Reduce(&local_time, &final_time, 7, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

  if(!pid) {
    printf("\nAverage reader time taken per process = %lf secs\n\nAverage mapper time taken per process = %lf secs\n", (final_time[READERS] / num_p), (final_time[MAPPERS] / num_p)); 
    printf("\nAverage sender time taken per process = %lf secs\n\nAverage receiver time taken per process = %lf secs\n", (final_time[SENDERS] / num_p), (final_time[RECEIVERS] / num_p));
    printf("\nAverage time taken to output local hashmap into a file = %lf secs\n\nAverage overall time taken on %d processes with %d reader-mapper thread pairs to complete the Map-Reduce operation = %lf secs\n", (final_time[OUTPUTFILE] / num_p), num_p, READER_THREADS, (final_time[OVERALL] / num_p));
    printf("\nAverage time spent in the critical section of the Mapper function per process = %lf secs\n", (final_time[6] / num_p));
  }

  return 0;
}

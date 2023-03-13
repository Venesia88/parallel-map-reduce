
#include <iostream>
#include <fstream>
#include <string>
#include <map>
#include <vector>
#include <algorithm>
#include <cmath>
#include <list>
#include <mpi.h>
#include <chrono>
#include <ctime>
#include <unistd.h>
#include <sstream>
#include <cstring>

#define WAIT 1
using namespace std;

struct threadMapPool {
    pthread_t* threads;
    pthread_mutex_t _mutexTask;
    string input_filename;
    string output_dir;
    pthread_mutex_t _mutexFree;
    list<int> tasks;
    int chunk_size;
    int rank;
    int num_reducer;
    int free_th = 0;
    bool terminate = false;
    list<int>finishTask;
    bool finish = false;
    int total = 0;
};

void reduceFunc(int reducerID, string out_dir, string job_name);
void threadMap(threadMapPool* pool, int rank, int delay);
void *mapFunc(void* arg);
bool check(int num_reducer, string file2, string output_dir, string job_name);
int main(int argc, char **argv)
{
    
    auto t1 = std::chrono::steady_clock::now();
    string job_name = argv[1];
    int num_reducer = stoi(argv[2]);
    int delay = stoi(argv[3]);
    string input_filename = argv[4];
    int chunk_size = stoi(argv[5]);
    string loc_filename = argv[6];
    string output_dir = argv[7];
    // string file2 = argv[8];

    //MPI Init
    int rank, size;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    int chunkID, nodeID;
    MPI_Status status;
    cpu_set_t cpuset;
    sched_getaffinity(0, sizeof(cpuset), &cpuset);
    int ncpus = CPU_COUNT(&cpuset);
    int num_thread = ncpus - 1;
   
    int flag_write = 0;
    int terminate = 0;
    string log_filename = output_dir + job_name + "-log.out";
    ofstream log_file(log_filename);
    


    if(rank == 0){
        map<int, pair<int, std::chrono::steady_clock::time_point>> lastReduceTask;
        bool task_pool[ncpus-1];
        list<pair<int, int>> nodeTask;
        std::hash<std::string> str_hash;

        ifstream loc_file(loc_filename);
        string line;
        
        map<int, vector<int>> Task;
        int lastChunk = 0;

        log_file << std::time(nullptr);
        log_file << ", Start_Job" << "," << job_name << ",";
        log_file << size << ",";
        log_file << ncpus << ",";
        log_file << num_reducer << ",";
        log_file << delay << ",";
        log_file << input_filename << ",";
        log_file << chunk_size << ",";
        log_file << loc_filename << ",";
        log_file << output_dir << endl;
        
        //read loc file
        while(getline(loc_file, line)){
            std::stringstream ss(line);
            ss >> chunkID >> nodeID;
            // cout << chunkID << ' ' << nodeID << endl;
            // Task[(nodeID % (size-1))+1].push_back(chunkID);
            nodeTask.push_back(make_pair((nodeID % (size-1))+1, chunkID));
            lastChunk = max(chunkID, lastChunk);
        }

        bool found = false;
        pair<int, int> first;
        bool sleep[size] = {false};
        int finishChunk;
        map<int, std::chrono::steady_clock::time_point> startChunk;

        //sendtask
        while(!nodeTask.empty()){
            found = false;
            MPI_Recv(&nodeID, 1, MPI_INT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, &status);
            MPI_Recv(&finishChunk, 1, MPI_INT, nodeID, 4, MPI_COMM_WORLD, &status);
            while(finishChunk != -1){
                auto end = std::chrono::steady_clock::now();
                std::chrono::duration<double> diff_map = end - startChunk[finishChunk];
                log_file << std::time(nullptr) << ", Complete_MapTask, " << finishChunk << ", ";
                log_file << diff_map.count() << endl;
                MPI_Recv(&finishChunk, 1, MPI_INT, nodeID, 4, MPI_COMM_WORLD, &status);
            }
            // cout << nodeID << endl;
            
            for(auto it = nodeTask.begin(); it!=nodeTask.end(); ++it){
                if((*it).first == nodeID){
                    log_file << std::time(nullptr) << ", Dispatch_MapTask, ";
                    log_file << (*it).second << ", " << nodeID << endl;
                    startChunk[(*it).second] = std::chrono::steady_clock::now();
                    MPI_Send(&((*it).second), 1, MPI_INT, nodeID, 0, MPI_COMM_WORLD);
                    
                    found = true;
                    nodeTask.erase(it);
                    break;
                }
            }
            
            if(!found && !nodeTask.empty()){
                chunkID = -2;
                MPI_Send(&chunkID, 1, MPI_INT, nodeID, 0, MPI_COMM_WORLD);
                first = nodeTask.front();
                    //get work from other node
                log_file << std::time(nullptr) << ", Dispatch_MapTask, ";
                log_file << first.second << ", " << nodeID << endl;
                startChunk[first.second] = std::chrono::steady_clock::now();
                MPI_Send(&first.second, 1, MPI_INT, nodeID, 7, MPI_COMM_WORLD);
                nodeTask.pop_front();
            }
        }

        //if no more left send finish to other node
        chunkID = -1;
        // cout << "send rank 0" << endl;
        for(int i = 1; i < size; ++i){
            MPI_Recv(&nodeID, 1, MPI_INT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, &status);
            MPI_Recv(&finishChunk, 1, MPI_INT, nodeID, 4, MPI_COMM_WORLD, &status);
            while(finishChunk != -1){
                auto end = std::chrono::steady_clock::now();
                std::chrono::duration<double> diff_map = end - startChunk[finishChunk];
                log_file << std::time(nullptr) << ", Complete_MapTask, " << finishChunk << ", ";
                log_file << diff_map.count() << endl;
                MPI_Recv(&finishChunk, 1, MPI_INT, nodeID, 4, MPI_COMM_WORLD, &status);
            }
            MPI_Send(&chunkID, 1, MPI_INT, nodeID, 0, MPI_COMM_WORLD);
            // cout << "send -1 to " << nodeID << endl;
        }

        for(int i = 1; i < size; ++i){
            MPI_Recv(&nodeID, 1, MPI_INT, MPI_ANY_SOURCE, 5, MPI_COMM_WORLD, &status);
            // cout << "recv rank " << nodeID << endl;
            MPI_Recv(&finishChunk, 1, MPI_INT, nodeID, 6, MPI_COMM_WORLD, &status);
            while(finishChunk != -2){
                auto end = std::chrono::steady_clock::now();
                std::chrono::duration<double> diff_map = end - startChunk[finishChunk];
                log_file << std::time(nullptr) << ", Complete_MapTask, " << finishChunk << ", ";
                log_file << diff_map.count() << endl;
                MPI_Recv(&finishChunk, 1, MPI_INT, nodeID, 6, MPI_COMM_WORLD, &status);
            }
        }

        // cout << "finish rank 0" << endl;
        ofstream mapOutfile[num_reducer];
        for(int i = 0; i < num_reducer; ++i)
            mapOutfile[i] = ofstream(output_dir + job_name + "-" + to_string(i)+".out");

        pair<string, int> wordCount;
        //start partition part
        auto start = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
        
        long long int count = 0;
        log_file << std::time(nullptr) << ", Start_Shuffle, ";
        for(int i = 1; i <= lastChunk; ++i){
            string path = output_dir + "chunkOut-"+to_string(i)+".out";
            char* char_array = new char[path.length() + 1];
            strcpy(char_array, path.c_str());
            ifstream chunkIn(path);

            while(getline(chunkIn, line)){
                std::stringstream ss(line);
                ss >> wordCount.first >> wordCount.second;
                count += 1;
                mapOutfile[str_hash(wordCount.first) % num_reducer] << wordCount.first << ' ' << wordCount.second << endl;
            }
            chunkIn.close();
            std::remove(char_array);
        }
        for(int i = 0; i < num_reducer; ++i)
            mapOutfile[i].close();

         log_file << count << endl;


        auto end = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
        log_file << std::time(nullptr) << ", Finish_Shuffle, " << end-start << endl;

        for(int i = 0; i < num_reducer; ++i){
            MPI_Recv(&nodeID, 1, MPI_INT, MPI_ANY_SOURCE, 3, MPI_COMM_WORLD, &status);
            if(lastReduceTask.count(nodeID)) {
                auto end_reduce = std::chrono::steady_clock::now();
                std::chrono::duration<double> diff = end_reduce - lastReduceTask[nodeID].second;
                log_file << std::time(nullptr) << ", Complete_ReduceTask, " << lastReduceTask[nodeID].first  << ", ";
                log_file << diff.count() << endl;
            }
            lastReduceTask[nodeID] = make_pair(i, std::chrono::steady_clock::now());
            log_file << std::time(nullptr) << ", Dispatch_ReduceTask, ";
            log_file << i << ", " << nodeID << endl;
            MPI_Send(&i, 1, MPI_INT, nodeID, 2, MPI_COMM_WORLD);
        }

        chunkID = -1;
        for(int i = 1; i < size; ++i){
            MPI_Recv(&nodeID, 1, MPI_INT, MPI_ANY_SOURCE, 3, MPI_COMM_WORLD, &status);
            auto end_reduce = std::chrono::steady_clock::now();
            std::chrono::duration<double> diff = end_reduce - lastReduceTask[nodeID].second;
            log_file << std::time(nullptr) << ", Complete_ReduceTask, " << lastReduceTask[nodeID].first  << ", ";
            log_file << diff.count() << endl;
            MPI_Send(&chunkID, 1, MPI_INT, nodeID, 2, MPI_COMM_WORLD);
        }

        // cout << check(num_reducer, file2, output_dir, job_name) << endl;
        auto t2 = std::chrono::steady_clock::now();
        std::chrono::duration<double> diff = t2-t1;

        log_file << std::time(nullptr) << ", Finish_Job, " << diff.count() << endl;
        log_file.close();
    }

    else {
        // MPI_Comm_split(MPI_COMM_WORLD,color,key,newcomm);
        threadMapPool* pool = new threadMapPool;
        pool->threads = new pthread_t[num_thread];
        pool->input_filename = input_filename;
        pool->chunk_size = chunk_size;
        pool->output_dir = output_dir;

        pthread_mutex_init(&(pool->_mutexFree), nullptr);
        pthread_mutex_init(&(pool->_mutexTask), nullptr);
        pool->rank = rank;
        pool->terminate = false;
        pool->total = 0;
        pool->free_th = 0;
        for(int i = 0; i < num_thread; ++i){
            pthread_create(&(pool->threads[i]), 0, mapFunc, (void*)pool);
            pthread_mutex_lock(&(pool->_mutexTask));
            ++pool->free_th;
            ++pool->total;
            pthread_mutex_unlock(&(pool->_mutexTask));
        }

        threadMap(pool, rank, delay);
        for(int i = 0; i < num_thread; ++i){
            pthread_join(pool->threads[i], NULL);
        }

        int reducerID;
        while(true){
            MPI_Send(&rank, 1, MPI_INT, 0, 3, MPI_COMM_WORLD);
            MPI_Recv(&reducerID, 1, MPI_INT, 0, 2, MPI_COMM_WORLD, &status);
            if(reducerID == -1) break;
            reduceFunc(reducerID, output_dir, job_name);
        }
        // MPI_Send(&rank, 1, MPI_INT, 0, 4, MPI_COMM_WORLD);
        // x.write("hi i am from mpi rank " + to_string(rank));
    }

    MPI_Finalize();
    
}

void threadMap(threadMapPool* pool, int rank, int delay){
    MPI_Status status;
    double now;
    int finishChunk;
    int chunkID = 0; 

    while(true){
        pthread_mutex_lock(&(pool->_mutexTask));
        if(!pool->free_th) {
            pthread_mutex_unlock(&(pool->_mutexTask));
            continue;
        }
        if(chunkID != -1){
            MPI_Send(&rank, 1, MPI_INT, 0, 1, MPI_COMM_WORLD);
            while(!pool->finishTask.empty()){
                finishChunk = pool->finishTask.front();
                // cout << "complete finish Chunk " << finishChunk << endl;
                pool->finishTask.pop_front(); 
                MPI_Send(&finishChunk, 1, MPI_INT, 0, 4, MPI_COMM_WORLD);
            }   
            finishChunk = -1;
            MPI_Send(&finishChunk, 1, MPI_INT, 0, 4, MPI_COMM_WORLD);
            
        }  
        else if(chunkID == -1 && (pool->free_th == pool->total)){
            
            MPI_Send(&rank, 1, MPI_INT, 0, 5, MPI_COMM_WORLD);
            // cout << "rank " << rank << " send -1 to master " << endl;
            while(!pool->finishTask.empty()){
                // cout << finishChunk << endl;
                finishChunk = pool->finishTask.front();
                // cout << "complete finish Chunk " << finishChunk << endl;
                pool->finishTask.pop_front(); 
                MPI_Send(&finishChunk, 1, MPI_INT, 0, 6, MPI_COMM_WORLD);
            }   
            finishChunk = -2;
            MPI_Send(&finishChunk, 1, MPI_INT, 0, 6, MPI_COMM_WORLD);
            pthread_mutex_unlock(&(pool->_mutexTask));
            break;
        }
        else {
            pthread_mutex_unlock(&(pool->_mutexTask));
            double sleep_time = MPI_Wtime();
            while(MPI_Wtime() - sleep_time < WAIT);
            continue;
        }
        pthread_mutex_unlock(&(pool->_mutexTask));

        MPI_Recv(&chunkID, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, &status);
        
        // cout << "rank = " << rank << " chunk " << chunkID << endl;

        if(chunkID == -2){
            // cout << "sleep" << endl;
            MPI_Recv(&chunkID, 1, MPI_INT, 0, 7, MPI_COMM_WORLD, &status);
            now = MPI_Wtime();
            while(MPI_Wtime() - now < delay);
            // continue;
        }
        else if(chunkID == -1){
            pthread_mutex_lock(&(pool->_mutexFree));
            pool->terminate = true;
            pthread_mutex_unlock(&(pool->_mutexFree));
            continue;
        }

        pthread_mutex_lock(&(pool->_mutexTask));
        // cout << rank << " add task " << chunkID << endl;
        pool->tasks.push_back(chunkID);
        // cout << "size " << pool->tasks.size() << endl;
        // for(int i = 0; i < pool->tasks.size(); ++i) 
        --pool->free_th;
        pthread_mutex_unlock(&(pool->_mutexTask));
    }
}

void* mapFunc(void* arg){
    int curID;
    string line;
    string sline;
    threadMapPool *pool = (threadMapPool*) arg;
    
    bool empty = false;
    while(true){
        curID = 0;
        // empty = true;
        pthread_mutex_lock(&(pool->_mutexTask));
        if(pool->terminate && pool->tasks.empty()) {
            pthread_mutex_unlock(&(pool->_mutexTask));
            break;
        }
        if(!pool->tasks.empty()){
            // empty = false;
            curID = (pool->tasks).front();
            
            (pool->tasks).pop_front();
            pool->finish = false;
        }
        pthread_mutex_unlock(&(pool->_mutexTask));

        if(curID > 0){
            // auto start = std::chrono::steady_clock::now();
            ifstream input_file(pool->input_filename);
            map<string, int> mapperResult;
            
            //read line
            for(int i = 0; i < (curID-1) * pool->chunk_size; ++i){
                getline(input_file, line);
            }

            for(int i = 0; i < pool->chunk_size; ++i){
                getline(input_file, line);

                istringstream ss(line);
                while(getline(ss, sline, ' ')){
                    if(mapperResult.count(sline)) mapperResult[sline] += 1;
                    else mapperResult[sline] = 1;
                }
            }
            input_file.close();
            ofstream mapOutfile(pool->output_dir + "chunkOut-"+to_string(curID)+".out");
            for (auto const& x : mapperResult) {
                mapOutfile << x.first << ' ' << x.second << endl;
            }
            mapOutfile.close();
            pthread_mutex_lock(&(pool->_mutexTask));
            ++pool->free_th;
            pool->finishTask.push_back(curID);
            // cout << "task " << curID << endl;
            
            if(pool->free_th == pool->total) pool->finish = true;
            pthread_mutex_unlock(&(pool->_mutexTask));
            curID = 0;
        }
    }
    return nullptr;
}
struct {
    bool operator()(pair<string, int> a, pair<string, int> b) const { return a.first < b.first; }
} customSort;

void reduceFunc(int reducerID, string out_dir, string job_name){
    vector<pair<string, int>> reducerTask;

    ifstream infile(out_dir + job_name + "-" + to_string(reducerID)+".out");
    string line;
    pair<string, int> wordCount;
    while(getline(infile, line)){
        std::stringstream ss(line);
        ss >> wordCount.first >> wordCount.second;
        reducerTask.push_back(wordCount);
    }

    infile.close();

    //sorting function
    sort(reducerTask.begin(), reducerTask.end(), customSort);

    //grouping function
    vector<string> orderedWord;
    map<string, vector<int>> group;
    // string s;
    for(auto word: reducerTask){
        //CHANGE GROUP FUNCTION HERE
        string s = word.first.substr(0, string::npos);
        if(group.count(s) == 0) orderedWord.push_back(s);
        group[s].push_back(word.second);
    }

    ofstream outfile(out_dir + job_name + "-" + to_string(reducerID)+".out");
    for(auto word: orderedWord){
        int sum = 0;
        for(auto cnt: group[word]){
            sum += cnt;
        }
        outfile << word << ' ' << sum << endl;
    }
    
    outfile.close();
}

bool check(int num_reducer, string file2, string output_dir, string job_name){

    string line;
    pair<string, int> wordCount;
    map<string, int> group;
    vector<string> orderedWord;

    for(int i = 0; i < num_reducer; ++i){
        ifstream infile(output_dir + job_name + "-" + to_string(i)+".out");

        while(getline(infile, line)){
            std::stringstream ss(line);
            ss >> wordCount.first >> wordCount.second;
            if(group.count(wordCount.first) == 0){
                group[wordCount.first] = wordCount.second;
                orderedWord.push_back(wordCount.first);
            } else group[wordCount.first] += wordCount.second;
        }
        infile.close();
    }

    sort(orderedWord.begin(), orderedWord.end());
    ofstream last(output_dir + "final.out");
    ifstream in2(file2);
    bool flag = false;
    string line1, line2;
    for(auto w: orderedWord){
        getline(in2, line2);
        last << w << " " << group[w] << endl;
        flag = true;
        string s = w + " " + to_string(group[w]);
        if(s != line2) return false;
    }
    last.close();
    
    in2.close();
    if(flag)
        return true;
    else {
        cout << "empty file" << endl;
        return false;
    }

}
#include "utree.h"

#define NR_LOAD         64000000
#define NR_OPERATIONS   64000000
#define LOAD_YCSB      "insert1_zipfian_64M_load.dat"     
#define RUN_YCSB       "insert1_zipfian_64M_run.dat"

#define FLOOR(x, y)    ((x) / (y))

uint64_t *loadKeys, *runKeys, *runTypes;
void loadWorkLoad();

int main(int argc, char **argv){
    int threadNum = atoi(argv[1]);

    loadKeys = new uint64_t[NR_LOAD];
    runKeys = new uint64_t[NR_OPERATIONS];
    runTypes = new uint64_t[NR_OPERATIONS];

    std::cout << "start load workload------------" << std::endl;
    loadWorkLoad();

    worker_id = 0;
    btree* bt = new btree(threadNum);
    std::cout << "warm up------------------------" << std::endl;
    for(int i=0; i<NR_LOAD; i++){
        bt->insert(loadKeys[i], reinterpret_cast<char*>(loadKeys[i]));
    }

    thread threads[threadNum];
    int range = FLOOR(NR_OPERATIONS, threadNum);
    std::cout << "start run----------------------" << std::endl;
    struct timeval startTime, endTime;
    gettimeofday(&startTime, NULL);
    for(int t=0; t<threadNum; t++){
        threads[t] = thread([=](){
            worker_id = t+1;
            int start = range*t;
            int end = ((t<threadNum-1)?start+range:NR_OPERATIONS);
            for (int ii = start; ii < end; ii++){
                if(runTypes[ii] == 1)
                    bt->insert(runKeys[ii], reinterpret_cast<char *>(runKeys[ii]));
                else
                    bt->search(runKeys[ii]);
            }
        });
    }

    for(int t=0; t<threadNum; t++)
        threads[t].join();
    gettimeofday(&endTime, NULL);
    double throughput = NR_OPERATIONS/((endTime.tv_sec + (double)(endTime.tv_usec) / 1000000) - (startTime.tv_sec + (double)(startTime.tv_usec) / 1000000));
    
    std::cout << "throughput: " << throughput << std::endl; 

    closeMemoryPool();
}

void loadWorkLoad(){
    ifstream ifs;
    ifs.open(LOAD_YCSB);
    std::string tmp;
    for(int i=0; i<NR_LOAD; i++){
        ifs >> tmp;
        ifs >> loadKeys[i];
    }
    ifs.close();

    ifs.open(RUN_YCSB);
    for(int i=0; i<NR_OPERATIONS; ++i){
        ifs >> tmp;
        if(tmp == "insert")
            runTypes[i] = 1;
        else if(tmp == "update")
            runTypes[i] = 2;
        else if(tmp == "delete")
            runTypes[i] = 3;
        else
            runTypes[i] = 0;
        ifs >> runKeys[i];
    }
    ifs.close();
}
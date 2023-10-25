#pragma once

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <iostream>
#include <thread>
#include <string.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <errno.h>

#define PAGE_SIZE 4096
thread_local int worker_id = -1;
int fd = -1;
static const uint64_t pool_size_set = (uint64_t)16 * 1024 * 1024 * 1024;

class CLMemPool{
public: 
    char *m_buf;
    size_t m_size;
    char *m_current;
    char *m_end;

public:
    CLMemPool(){
        m_buf = nullptr;
        m_size = 0;
        m_current = nullptr;
        m_end = nullptr;
    }

    void initialize(char* start, size_t size){
        m_buf = start;
        m_size = size;
        m_current = start;
        m_end = start + size;
    }

    ~CLMemPool(){
        m_buf = nullptr;
        m_size = 0;
        m_current = nullptr;
        m_end = nullptr;
    }

    void* Allocate(size_t size){
        if (m_current + size <= m_end){
            register char *p;
            p = m_current;
            m_current += size;
            return (void *)p;
        }
        return nullptr;
    }
};

class CLThreadPMPool{
public: 
    CLMemPool *m_pools;
    int m_thread_num;
    char *m_buf;
    size_t m_pool_size;

public:
    CLThreadPMPool(){
        m_pools = nullptr;
        m_buf = nullptr;
        m_pool_size = 0;
        m_thread_num = 0;
    }

    void initialize(size_t pool_size, int threadNum){
        m_thread_num = threadNum;

        m_pools = new CLMemPool[threadNum];
        size_t sizeOfPool = (pool_size/(threadNum+threadNum-2)/PAGE_SIZE)*PAGE_SIZE;
        m_pool_size = sizeOfPool*(threadNum+threadNum-2);
        
#ifdef MMAP    
        fd = open("largefile", O_RDWR);
        if (fd == -1) {
            perror("open");
            return;
        }
        char* mapped = (char*) mmap(nullptr, m_pool_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (mapped == MAP_FAILED) {
            perror("mmap");
            close(fd);
            return;
        }
        m_buf = mapped;
#else
        void* tmp_buf;
        int resAlloc = posix_memalign(&tmp_buf,64,m_pool_size);
        if(resAlloc){
            perror(nullptr);
            exit(-1);
        }
        m_buf = (char*) tmp_buf;
#endif
        m_pools[0].initialize(m_buf, sizeOfPool*(threadNum-1));
        for (int i = 1; i < threadNum; i++)
            m_pools[i].initialize(m_buf + (i-1+threadNum-1) * sizeOfPool, sizeOfPool);
    }

    ~CLThreadPMPool(){
#ifdef MMAP
        munmap(m_buf, m_pool_size);
        close(fd);
#else
        free(m_buf);
#endif
        m_buf = nullptr;
        m_pool_size = 0;
        m_thread_num = 0;
        delete [] m_pools;
        m_pools = nullptr;
    }
};

CLThreadPMPool* pmAllocator = new CLThreadPMPool();

void initializeMemoryPool(int threadNum, bool isRecover = false){
    pmAllocator->initialize(pool_size_set, threadNum);
}

void closeMemoryPool(){
    pmAllocator->~CLThreadPMPool();
}

inline void persist(char *addr, int len){
#ifdef MMAP 
    char* aligned_addr = (char*)(~((uintptr_t)0xFFF) & (uintptr_t)addr);
    if (msync(aligned_addr, 4096, MS_SYNC) == -1) {
        perror("msync");
        std::cout << "msync: error" << std::endl;
    }
    return;
#endif
}

void *alloc(size_t size) {
  void* ret;
  ret = pmAllocator->m_pools[worker_id].Allocate(size);
  return ret;
}
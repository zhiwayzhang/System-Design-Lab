#include <cassert>
#include <climits>
#include <fstream>
#include <iostream> 
#include <math.h>
#include <mutex>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <vector>
#include "allocator.h"

#define PAGESIZE 520
#define CACHE_LINE_SIZE 64 
#define IS_FORWARD(c) (c % 2 == 0)

using entry_key_t = int64_t;

const uint64_t deletedSet = (uint64_t)1 << 63;
const uint64_t deleteMask = deletedSet - 1;
const uint64_t ptrSet = (uint64_t)(0xffffffffffff);
const uint64_t ptrMask = (uint64_t)(0xffff) << 48;
const uint64_t versionSet = (uint64_t)(0x7fff) << 48;
const uint64_t versionMask = deletedSet | ptrSet;

using namespace std;

#define CAS(_p, _u, _v)                                             \
  (__atomic_compare_exchange_n(_p, _u, _v, false, __ATOMIC_ACQUIRE, \
                               __ATOMIC_ACQUIRE))
 
class list_node_t {
public:
  uint64_t ptr;  
  entry_key_t key;
  uint64_t size;  
  uint64_t next;

  inline void acquireVersionLock(){
    uint64_t oldValue = 0;
    uint64_t newValue = 0;
    do{
      while(true){
        oldValue = __atomic_load_n(&next, __ATOMIC_ACQUIRE);
        if(((oldValue & versionSet) >> 48) % 2 == 0)
          break;
      }
      newValue = ((((oldValue & versionSet) >> 48) + 1) << 48) | (oldValue & versionMask);
    }while(!CAS(&next, &oldValue, newValue));
  }

  inline void releaseVersion(){
    uint64_t value = __atomic_load_n(&next, __ATOMIC_ACQUIRE);
    if((value & versionSet) == versionSet)
      value = ((((value & versionSet) >> 48) + 1) << 48) | (value & versionMask);
    else
      value = value & versionMask;
    __atomic_store_n(&next, value, __ATOMIC_RELEASE);
  }
};

class page;

class btree{
  private:

  public:
    int height;
    char* root;

    list_node_t *list_head = NULL;
    btree(int threadNum);
    ~btree();
    void setNewRoot(char *);
    void btree_insert_pred(entry_key_t, char*, char **pred, bool*);
    void btree_insert_internal(char *, entry_key_t, char *, uint32_t);
    char *btree_search(entry_key_t);
    char *btree_search_pred(entry_key_t, bool *f, char**, bool);
    char *btree_search_pred_test(entry_key_t, bool *f, char**, bool, page**);
    void insert(entry_key_t, char*); 
    char* search(entry_key_t); 

    friend class page;
};


class header{
  private:
    page* leftmost_ptr;         
    page* sibling_ptr;          
    page* pred_ptr;            
    uint32_t level;             
    uint8_t switch_counter;    
    uint8_t is_deleted;         
    int16_t last_index;        
    std::mutex *mtx;            

    friend class page;
    friend class btree;

  public:
    header() {
      mtx = new std::mutex();

      leftmost_ptr = NULL;  
      sibling_ptr = NULL;
      pred_ptr = NULL;
      switch_counter = 0;
      last_index = -1;
      is_deleted = false;
    }

    ~header() {
      delete mtx;
    }
};

class entry{ 
  private:
    entry_key_t key; 
    char* ptr; 

  public :
    entry(){
      key = LONG_MAX;
      ptr = NULL;
    }

    friend class page;
    friend class btree;
};

const int cardinality = (PAGESIZE-sizeof(header))/sizeof(entry);

class page{
  private:
    header hdr;  
    entry records[cardinality]; 

  public:
    friend class btree;

    page(uint32_t level = 0) {
      hdr.level = level;
      records[0].ptr = NULL;
    }

    page(page* left, entry_key_t key, page* right, uint32_t level = 0) {
      hdr.leftmost_ptr = left;  
      hdr.level = level;
      records[0].key = key;
      records[0].ptr = (char*) right;
      records[1].ptr = NULL;

      hdr.last_index = 0;
    }

    void *operator new(size_t size) {
      void *ret;
      posix_memalign(&ret,64,size);
      return ret;
    }

    inline int count() {
      uint8_t previous_switch_counter;
      int count = 0;
      do {
        previous_switch_counter = hdr.switch_counter;
        count = hdr.last_index + 1;

        while(count >= 0 && records[count].ptr != NULL) {
          if(IS_FORWARD(previous_switch_counter))
            ++count;
          else
            --count;
        } 

        if(count < 0) {
          count = 0;
          while(records[count].ptr != NULL) {
            ++count;
          }
        }

      } while(previous_switch_counter != hdr.switch_counter);

      return count;
    }

    inline void insert_key(entry_key_t key, char* ptr, int *num_entries, bool flush = true,
        bool update_last_index = true) {
          // update switch_counter
          if(!IS_FORWARD(hdr.switch_counter))
            ++hdr.switch_counter;

          // FAST
          if(*num_entries == 0) {  // this page is empty
            entry* new_entry = (entry*) &records[0];
            entry* array_end = (entry*) &records[1];
            new_entry->key = (entry_key_t) key;
            new_entry->ptr = (char*) ptr;

            array_end->ptr = (char*)NULL;

          }
          else {
            int i = *num_entries - 1, inserted = 0;
            records[*num_entries+1].ptr = records[*num_entries].ptr; 
          

          // FAST
          for(i = *num_entries - 1; i >= 0; i--) {
            if(key < records[i].key ) {
              records[i+1].ptr = records[i].ptr;
              records[i+1].key = records[i].key;
            }
            else{
              records[i+1].ptr = records[i].ptr;
              records[i+1].key = key;
              records[i+1].ptr = ptr;
              inserted = 1;
              break;
            }
          }
          if(inserted==0){
            records[0].ptr =(char*) hdr.leftmost_ptr;
            records[0].key = key;
            records[0].ptr = ptr;
          }
        }

        if(update_last_index) {
          hdr.last_index = *num_entries;
        }
        ++(*num_entries);
      }

    page *store(btree* bt, char* left, entry_key_t key, char* right,
       bool flush, bool with_lock, page *invalid_sibling = NULL) {
        if(with_lock) {
          hdr.mtx->lock(); 
        }
        if(hdr.is_deleted) {
          if(with_lock) {
            hdr.mtx->unlock();
          }

          return NULL;
        }

        register int num_entries = count();

        for (int i = 0; i < num_entries; i++)
          if (key == records[i].key) {
            records[i].ptr = right;
            if (with_lock)
              hdr.mtx->unlock();
            return this;
          }

        if(hdr.sibling_ptr && (hdr.sibling_ptr != invalid_sibling)) {
          if(key > hdr.sibling_ptr->records[0].key) {
            if(with_lock) { 
              hdr.mtx->unlock();
            }
            return hdr.sibling_ptr->store(bt, NULL, key, right, 
                true, with_lock, invalid_sibling);
          }
        }

        if(num_entries < cardinality - 1) {
          insert_key(key, right, &num_entries, flush);

          if(with_lock) {
            hdr.mtx->unlock(); 
          }

          return this;
        }
        else {
          page* sibling = new page(hdr.level); 
          register int m = (int) ceil(num_entries/2);
          entry_key_t split_key = records[m].key;

          int sibling_cnt = 0;
          if(hdr.leftmost_ptr == NULL){
            for(int i=m; i<num_entries; ++i){ 
              sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt, false);
            }
          }
          else{ 
            for(int i=m+1;i<num_entries;++i){ 
              sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt, false);
            }
            sibling->hdr.leftmost_ptr = (page*) records[m].ptr;
          }

          sibling->hdr.sibling_ptr = hdr.sibling_ptr;
          sibling->hdr.pred_ptr = this;
          if (sibling->hdr.sibling_ptr != NULL)
            sibling->hdr.sibling_ptr->hdr.pred_ptr = sibling;
          hdr.sibling_ptr = sibling;

          if(IS_FORWARD(hdr.switch_counter))
            hdr.switch_counter += 2;
          else
            ++hdr.switch_counter;
          records[m].ptr = NULL;
          hdr.last_index = m - 1;
          num_entries = hdr.last_index + 1;

          page *ret;

          if(key < split_key) {
            insert_key(key, right, &num_entries);
            ret = this;
          }
          else {
            sibling->insert_key(key, right, &sibling_cnt);
            ret = sibling;
          }

          if(bt->root == (char *)this) { 
            page* new_root = new page((page*)this, split_key, sibling, 
                hdr.level + 1);
            bt->setNewRoot((char *)new_root);

            if(with_lock) {
              hdr.mtx->unlock(); 
            }
          }
          else {
            if(with_lock) {
              hdr.mtx->unlock(); 
            }
            bt->btree_insert_internal(NULL, split_key, (char *)sibling, 
                hdr.level + 1);
          }

          return ret;
        }

      }

    inline void insert_key(entry_key_t key, char* ptr, int *num_entries, char **pred, bool flush = true,
          bool update_last_index = true) {
        if(!IS_FORWARD(hdr.switch_counter))
          ++hdr.switch_counter;

        if(*num_entries == 0) {  
          entry* new_entry = (entry*) &records[0];
          entry* array_end = (entry*) &records[1];
          new_entry->key = (entry_key_t) key;
          new_entry->ptr = (char*) ptr;

          array_end->ptr = (char*)NULL;

          if (hdr.pred_ptr != NULL)
            *pred = hdr.pred_ptr->records[hdr.pred_ptr->count() - 1].ptr;
        }
        else {
          int i = *num_entries - 1, inserted = 0;
          records[*num_entries+1].ptr = records[*num_entries].ptr; 
          
          for(i = *num_entries - 1; i >= 0; i--) {
            if(key < records[i].key ) {
              records[i+1].ptr = records[i].ptr;
              records[i+1].key = records[i].key;
            }
            else{
              records[i+1].ptr = records[i].ptr;
              records[i+1].key = key;
              records[i+1].ptr = ptr;
              *pred = records[i].ptr;
              inserted = 1;
              break;
            }
          }
          if(inserted==0){
            records[0].ptr =(char*) hdr.leftmost_ptr;
            records[0].key = key;
            records[0].ptr = ptr;
            if (hdr.pred_ptr != NULL)
              *pred = hdr.pred_ptr->records[hdr.pred_ptr->count() - 1].ptr;
          }
        }

        if(update_last_index) {
          hdr.last_index = *num_entries;
        }
        ++(*num_entries);
      }

    page *store(btree* bt, char* left, entry_key_t key, char* right,
       bool flush, bool with_lock, char **pred, page *invalid_sibling = NULL) {
        if(with_lock) {
          hdr.mtx->lock(); 
        }
        if(hdr.is_deleted) {
          if(with_lock) {
            hdr.mtx->unlock();
          }
          return NULL;
        }

        register int num_entries = count();

        for (int i = 0; i < num_entries; i++)
          if (key == records[i].key) {
            *pred = records[i].ptr;
            if (with_lock)
              hdr.mtx->unlock();
            return NULL;
          }

        if(hdr.sibling_ptr && (hdr.sibling_ptr != invalid_sibling)) {
          if(key > hdr.sibling_ptr->records[0].key) {
            if(with_lock) { 
              hdr.mtx->unlock();
            }
            return hdr.sibling_ptr->store(bt, NULL, key, right, 
                true, with_lock, pred, invalid_sibling);
          }
        }

        if(num_entries < cardinality - 1) {
          insert_key(key, right, &num_entries, pred);

          if(with_lock) {
            hdr.mtx->unlock(); 
          }

          return this;
        }else {
          page* sibling = new page(hdr.level); 
          register int m = (int) ceil(num_entries/2);
          entry_key_t split_key = records[m].key;

          int sibling_cnt = 0;
          if(hdr.leftmost_ptr == NULL){ 
            for(int i=m; i<num_entries; ++i){ 
              sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt, false);
            }
          }
          else{ 
            for(int i=m+1;i<num_entries;++i){ 
              sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt, false);
            }
            sibling->hdr.leftmost_ptr = (page*) records[m].ptr; 
          }

          sibling->hdr.sibling_ptr = hdr.sibling_ptr;
          sibling->hdr.pred_ptr = this;
          if (sibling->hdr.sibling_ptr != NULL)
            sibling->hdr.sibling_ptr->hdr.pred_ptr = sibling;
          hdr.sibling_ptr = sibling;

          if(IS_FORWARD(hdr.switch_counter))
            hdr.switch_counter += 2;
          else
            ++hdr.switch_counter;
          records[m].ptr = NULL;
          hdr.last_index = m - 1;
          num_entries = hdr.last_index + 1;

          page *ret;

          if(key < split_key) {
            insert_key(key, right, &num_entries, pred);
            ret = this;
          }
          else {
            sibling->insert_key(key, right, &sibling_cnt, pred);
            ret = sibling;
          }

          if(bt->root == (char *)this) { 
            page* new_root = new page((page*)this, split_key, sibling, 
                hdr.level + 1);
            bt->setNewRoot((char *)new_root);

            if(with_lock) {
              hdr.mtx->unlock(); 
            }
          }
          else {
            if(with_lock) {
              hdr.mtx->unlock(); 
            }
            bt->btree_insert_internal(NULL, split_key, (char *)sibling, 
                hdr.level + 1);
          }

          return ret;
        }

      }

    char *linear_search(entry_key_t key) {
      int i = 1;
      uint8_t previous_switch_counter;
      char *ret = NULL;
      char *t; 
      entry_key_t k;

      if(hdr.leftmost_ptr == NULL) { 
        do {
          previous_switch_counter = hdr.switch_counter;
          ret = NULL;

          if(IS_FORWARD(previous_switch_counter)) { 
            if((k = records[0].key) == key) { 
              if((t = records[0].ptr) != NULL) {
                if(k == records[0].key) {
                  ret = t;
                  continue;
                }
              }
            }

            for(i=1; records[i].ptr != NULL; ++i) { 
              if((k = records[i].key) == key) {
                if(records[i-1].ptr != (t = records[i].ptr)) {
                  if(k == records[i].key) {
                    ret = t;
                    break;
                  }
                }
              }
            }
          }
          else { 
            for(i = count() - 1; i > 0; --i) {
              if((k = records[i].key) == key) {
                if(records[i - 1].ptr != (t = records[i].ptr) && t) {
                  if(k == records[i].key) {
                    ret = t;
                    break;
                  }
                }
              }
            }

            if(!ret) {
              if((k = records[0].key) == key) {
                if(NULL != (t = records[0].ptr) && t) {
                  if(k == records[0].key) {
                    ret = t;
                    continue;
                  }
                }
              }
            }
          }
        } while(hdr.switch_counter != previous_switch_counter);

        if(ret) {
          return ret;
        }

        if((t = (char *)hdr.sibling_ptr) && key >= ((page *)t)->records[0].key)
          return t;

        return NULL;
      }
      else { 
        do {
          previous_switch_counter = hdr.switch_counter;
          ret = NULL;

          if(IS_FORWARD(previous_switch_counter)) {
            if(key < (k = records[0].key)) {
              if((t = (char *)hdr.leftmost_ptr) != records[0].ptr) { 
                ret = t;
                continue;
              }
            }

            for(i = 1; records[i].ptr != NULL; ++i) { 
              if(key < (k = records[i].key)) { 
                if((t = records[i-1].ptr) != records[i].ptr) {
                  ret = t;
                  break;
                }
              }
            }

            if(!ret) {
              ret = records[i - 1].ptr; 
              continue;
            }
          }
          else {
            for(i = count() - 1; i >= 0; --i) {
              if(key >= (k = records[i].key)) {
                if(i == 0) {
                  if((char *)hdr.leftmost_ptr != (t = records[i].ptr)) {
                    ret = t;
                    break;
                  }
                }
                else {
                  if(records[i - 1].ptr != (t = records[i].ptr)) {
                    ret = t;
                    break;
                  }
                }
              }
            }
          }
        } while(hdr.switch_counter != previous_switch_counter);

        if((t = (char *)hdr.sibling_ptr) != NULL) {
          if(key >= ((page *)t)->records[0].key)
            return t;
        }

        if(ret) {
          return ret;
        }
        else
          return (char *)hdr.leftmost_ptr;
      }

      return NULL;
    }

    char *linear_search_pred(entry_key_t key, char **pred, bool debug=false) {
      int i = 1;
      uint8_t previous_switch_counter;
      char *ret = NULL;
      char *t; 
      entry_key_t k, k1;

      if(hdr.leftmost_ptr == NULL) { 
        do {
          previous_switch_counter = hdr.switch_counter;
          ret = NULL;

          if(IS_FORWARD(previous_switch_counter)) {
            k = records[0].key;
            if (key < k) {
              if (hdr.pred_ptr != NULL){
                *pred = hdr.pred_ptr->records[hdr.pred_ptr->count() - 1].ptr;
              }
            }
            if (key > k){
              *pred = records[0].ptr;
            }
              

            if(k == key) {
              if (hdr.pred_ptr != NULL) {
                *pred = hdr.pred_ptr->records[hdr.pred_ptr->count() - 1].ptr;
              }
              if((t = records[0].ptr) != NULL) {
                if(k == records[0].key) {
                  ret = t;
                  continue;
                }
              }
            }

            for(i=1; records[i].ptr != NULL; ++i) { 
              k = records[i].key;
              if (k < key){
                *pred = records[i].ptr;
              }
              if(k == key) {
                if(records[i-1].ptr != (t = records[i].ptr)) {
                  if(k == records[i].key) {
                    ret = t;
                    break;
                  }
                }
              }
            }
          }else { 
            bool once = true;
            
            if(records[count()-1].key < key){
              *pred = records[count()-1].ptr;
              once = false;
            }

            for (i = count() - 1; i > 0; --i) {
              k = records[i].key;
              k1 = records[i - 1].key;
              if (k1 < key && once) {
                *pred = records[i - 1].ptr;
                once = false;
              }
              if(k == key) {
                if(records[i - 1].ptr != (t = records[i].ptr) && t) {
                  if(k == records[i].key) {
                    ret = t;
                    break;
                  }
                }
              }
            }

            if(!ret) {
              k = records[0].key;
              if (key < k){
                if (hdr.pred_ptr != NULL){
                  *pred = hdr.pred_ptr->records[hdr.pred_ptr->count() - 1].ptr;
                }
              }
              if (key > k && once)
                *pred = records[0].ptr;
              if(k == key) {
                if (hdr.pred_ptr != NULL) {
                  *pred = hdr.pred_ptr->records[hdr.pred_ptr->count() - 1].ptr;
                }
                if(NULL != (t = records[0].ptr) && t) {
                  if(k == records[0].key) {
                    ret = t;
                    continue;
                  }
                }
              }
            }
          }
        } while(hdr.switch_counter != previous_switch_counter);

        if(ret) {
          return ret;
        }

        if((t = (char *)hdr.sibling_ptr) && key >= ((page *)t)->records[0].key)
          return t;

        return NULL;
      }
      else { 
        do {
          previous_switch_counter = hdr.switch_counter;
          ret = NULL;

          if(IS_FORWARD(previous_switch_counter)) {
            if(key < (k = records[0].key)) {
              if((t = (char *)hdr.leftmost_ptr) != records[0].ptr) { 
                ret = t;
                continue;
              }
            }

            for(i = 1; records[i].ptr != NULL; ++i) { 
              if(key < (k = records[i].key)) { 
                if((t = records[i-1].ptr) != records[i].ptr) {
                  ret = t;
                  break;
                }
              }
            }

            if(!ret) {
              ret = records[i - 1].ptr; 
              continue;
            }
          }
          else {
            for(i = count() - 1; i >= 0; --i) {
              if(key >= (k = records[i].key)) {
                if(i == 0) {
                  if((char *)hdr.leftmost_ptr != (t = records[i].ptr)) {
                    ret = t;
                    break;
                  }
                }
                else {
                  if(records[i - 1].ptr != (t = records[i].ptr)) {
                    ret = t;
                    break;
                  }
                }
              }
            }
          }
        } while(hdr.switch_counter != previous_switch_counter);

        if((t = (char *)hdr.sibling_ptr) != NULL) {
          if(key >= ((page *)t)->records[0].key)
            return t;
        }

        if(ret) {
          return ret;
        }
        else
          return (char *)hdr.leftmost_ptr;
      }

      return NULL;
    }
};

btree::btree(int threadNum = 0){
  initializeMemoryPool(threadNum+1);

  root = (char*)new page();
  list_head = (list_node_t *)alloc(sizeof(list_node_t));
  list_head->next = NULL;
  height = 1; 
}

btree::~btree() { 

}

void btree::setNewRoot(char *new_root) {
  this->root = (char*)new_root;
  ++height;
}

char *btree::btree_search_pred(entry_key_t key, bool *f, char **prev, bool debug=false){
  page* p = (page*)root;

  while(p->hdr.leftmost_ptr != NULL) {
    p = (page *)p->linear_search(key);
  }

  page *t;
  while((t = (page *)p->linear_search_pred(key, prev, debug)) == p->hdr.sibling_ptr) {
    p = t;
    if(!p) {
      break;
    }
  }

  if(!t) {
    *f = false;
    return NULL;
  }

  *f = true;
  return (char *)t;
}

char *btree::btree_search_pred_test(entry_key_t key, bool *f, char **prev, bool debug=false, page** testPage=NULL){
  page* p = (page*)root;

  while(p->hdr.leftmost_ptr != NULL) {
    p = (page *)p->linear_search(key);
  }

  *testPage = (page*)p;

  page *t;
  while((t = (page *)p->linear_search_pred(key, prev, debug)) == p->hdr.sibling_ptr) {
    *testPage = (page*)p;
    p = t;
    if(!p) {
      break;
    }
  }

  if(!t) {
    *f = false;
    return NULL;
  }

  *f = true;
  return (char *)t;
}

char *btree::search(entry_key_t key) {
  bool f = false;
  char *prev;
  char *ptr = btree_search_pred(key, &f, &prev);
  if (f) {
    list_node_t *n = (list_node_t *)ptr;
    if (n->ptr != 0)
      return (char *)n->ptr; 
  }
  return NULL;
}

void btree::btree_insert_pred(entry_key_t key, char* right, char **pred, bool *update){ 
  page* p = (page*)root;

  while(p->hdr.leftmost_ptr != NULL) { 
    p = (page*)p->linear_search(key);
  }
  *pred = NULL;
  if(!p->store(this, NULL, key, right, true, true, pred)) {  
    *update = true;
  } else {
    *update = false;
  }
}

void btree::insert(entry_key_t key, char *right) {
  int retry = 0;
  bool hasFound;
  list_node_t *prev = NULL, *cur = NULL;
  list_node_t *n;
  page* testPage = NULL;
retryinsert:
  if(retry > 10){
    return;
  }

  cur = (list_node_t*)btree_search_pred_test(key, &hasFound, (char**)&prev, false, &testPage);

  if(cur){
    cur->acquireVersionLock();
    cur->ptr = (uint64_t)right;
    persist((char*)cur, sizeof(list_node_t));
    cur->releaseVersion();
  }else{
    if(retry == 0){
      n = (list_node_t *)alloc(sizeof(list_node_t));
      n->next = NULL;
      n->key = key;
      n->ptr = (uint64_t)right;
    }
    if(list_head->next != NULL){
      if(prev == NULL)
        prev = list_head;
      
      list_node_t *next = (list_node_t*)__atomic_load_n(&(prev->next), __ATOMIC_ACQUIRE);

      if(((uint64_t)next & deletedSet) != 0){
        retry++;
        goto retryinsert;
      }

      uint64_t oldValue = (uint64_t)next;
      next = (list_node_t*)((uint64_t)next & ptrSet);

      if((prev == list_head || (prev != list_head && prev->key < key)) && (next == NULL || (next != NULL && next->key > key))){
        n->next = (uint64_t)next;
        persist((char*)n, sizeof(list_node_t));
        if(!CAS(&(prev->next), &oldValue, n)){
          retry++;
          goto retryinsert;
        }

        persist((char*)prev, sizeof(list_node_t));
        prev = NULL;
        testPage->store(this, nullptr, key, (char*)n, true, true, (char**)&prev);
      }else{
        retry++;
        goto retryinsert;
      }
    }else{
      if(!__sync_bool_compare_and_swap(&(list_head->next), NULL, n)){
        retry++;
        goto retryinsert;
      }
      btree_insert_pred(key, (char*)n, (char**)&prev, &hasFound);
    }
  }
}

void btree::btree_insert_internal(char *left, entry_key_t key, char *right, uint32_t level) {
  if(level > ((page *)root)->hdr.level)
    return;

  page *p = (page *)this->root;

  while(p->hdr.level > level) 
    p = (page *)p->linear_search(key);

  if(!p->store(this, NULL, key, right, true, true)) {
    btree_insert_internal(left, key, right, level);
  }
}
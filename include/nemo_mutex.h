#ifndef NEMO_INCLUDE_NEMO_MUTEX_
#define NEMO_INCLUDE_NEMO_MUTEX_
#include <pthread.h>

namespace nemo {

class MutexLock {
    public:
        explicit MutexLock(pthread_mutex_t *mu)
            : mu_(mu)  {
                pthread_mutex_lock(this->mu_);
            }
        ~MutexLock() { pthread_mutex_unlock(this->mu_); }

    private:
        pthread_mutex_t *const mu_;
        // No copying allowed
        MutexLock(const MutexLock&);
        void operator=(const MutexLock&);
};

}
#endif

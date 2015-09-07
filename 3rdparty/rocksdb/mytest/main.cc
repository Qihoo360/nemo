
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "rocksdb/db.h"
#include "rocksdb/utilities/db_ttl.h"

int main()
{
    rocksdb::DBWithTTL* db;
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DBWithTTL::Open(options, "./testdb", &db);
    //rocksdb::Status status = rocksdb::DBWithTTL::OpenWithKeyTTL(options, "./testdb", &db);
    assert(status.ok());

    rocksdb::Status s;
    s = db->PutWithKeyTTL(rocksdb::WriteOptions(), "aa", "vaa", 10);

    std::string value;
    s = db->Get(rocksdb::ReadOptions(), "aa", &value);
    printf("%s \n", value.c_str());

    sleep(3);

    int32_t ttl;
    db->GetKeyTTL(rocksdb::ReadOptions(), "aa", &ttl);
    printf("%d \n", ttl);

    sleep(2);
    db->GetKeyTTL(rocksdb::ReadOptions(), "aa", &ttl);
    printf("%d \n", ttl);

    for (int i = 0; i < 5; i++) {
        sleep(2);
        db->GetKeyTTL(rocksdb::ReadOptions(), "aa", &ttl);
        printf("ttl: %d \n", ttl);

        s = db->Get(rocksdb::ReadOptions(), "aa", &value);
        printf ("Get(aa) return %s\n", s.ToString().c_str());
        if (s.ok()) {
            printf("Get value:%s \n\n", value.c_str());
        }
    }

    s = db->Put(rocksdb::WriteOptions(), "bb", "vbb");
    if (!s.ok()) {
        printf ("Put return %s\n", s.ToString().c_str());
    } else {
        for (int i = 0; i < 5; i++) {
            sleep(2);
            db->GetKeyTTL(rocksdb::ReadOptions(), "bb", &ttl);
            printf("ttl: %d \n", ttl);

            s = db->Get(rocksdb::ReadOptions(), "bb", &value);
            printf ("Get(bb) return %s\n", s.ToString().c_str());
            if (s.ok()) {
                printf("Get value:%s \n\n", value.c_str());
            }
        }
    }

    delete db;
    return 0;
}

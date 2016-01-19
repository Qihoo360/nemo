#ifndef NEMO_INCLUDE_NEMO_BACKUPABLE_H_
#define NEMO_INCLUDE_NEMO_BACKUPABLE_H_
#include <string>
#include <map>
#include <atomic>
#include "rocksdb/db.h"
#include "rocksdb/utilities/backupable_db.h"
#include "nemo.h"
#include "nemo_const.h"
#include "nemo_mutex.h"

namespace nemo {
    const std::string DEFAULT_BK_PATH = "dump"; //Default backup root dir
    const std::string DEFAULT_RS_PATH = "db";   //Default restore root dir

    struct BackupableOptions {
        // Where to keep the backup files. Has to be different than dbname_
        // Best to set this to dbname_ + "/backups"
        // Required
        std::string backup_dir;

        // If share_table_files == true, backup will assume that table files with
        // same name have the same contents. This enables incremental backups and
        // avoids unnecessary data copies.
        // If share_table_files == false, each backup will be on its own and will
        // not share any data with other backups.
        // default: true
        bool share_table_files;

        // If sync == true, we can guarantee you'll get consistent backup even
        // on a machine crash/reboot. Backup process is slower with sync enabled.
        // If sync == false, we don't guarantee anything on machine reboot. However,
        // chances are some of the backups are consistent.
        // Default: true
        bool sync;

        explicit BackupableOptions(const std::string& _backup_dir,
                bool _share_table_files = true,
                bool _sync = false)
            : backup_dir(_backup_dir),
            share_table_files(_share_table_files),
            sync(_sync) {}
    };

    typedef uint32_t BackupID;

    // Arguments which will used by BackupSave Thread
    // p_nemo for nemo handler
    // p_engine for BackupEngine handler
    // key_type kv, hash, list, set or zset
    struct BackupSaveArgs {
        void *p_nemo;
        void *p_engine;
        const std::string key_type;

        BackupSaveArgs(void *_p_nemo, void *_p_engine, const std::string &_key_type)
            : p_nemo(_p_nemo), p_engine(_p_engine), key_type(_key_type) {};
    };

    // Arguments which will used by BackupRestore Thread
    // p_engine for BackupEngine handler
    // key_type kv, hash, list, set or zset
    struct BackupRestoreArgs {
        void *p_engine;
        const std::string key_type;
        BackupID backup_id;
        const std::string db_dir;

        BackupRestoreArgs(void *_p_engine, const std::string &_key_type, 
                BackupID _backup_id, const std::string &_db_dir)
            : p_engine(_p_engine), key_type(_key_type), 
            backup_id(_backup_id), db_dir(_db_dir){};
    };

    typedef std::vector<std::unique_ptr<rocksdb::LogFile>> VectorLogPtr;
    struct BackupContent {
        std::vector<std::string> live_files;
        VectorLogPtr live_wal_files;
        uint64_t manifest_file_size = 0;
        uint64_t sequence_number = 0;
    };

    class BackupEngine {
        public:
            ~BackupEngine();
            static Status Open(const BackupableOptions& options,
                    BackupEngine** backup_engine_ptr);

            Status SetBackupContent(nemo::Nemo *db);
            
            Status CreateNewBackup(nemo::Nemo *db);
            
            void StopBackup();
    
            void PrintBackupContent();

            Status RestoreDBFromBackup(
                    BackupID backup_id, const std::string& db_dir);

            Status RestoreDBFromLatestBackup(
                    const std::string& db_dir) {
                return RestoreDBFromBackup(latest_backup_id_, db_dir);
            }
            Status CreateNewBackupSpecify(nemo::Nemo *db, const std::string &type);
            Status RestoreFromBackupSpecify(const std::string &type, BackupID id, const std::string &db_dir);
        private:
            BackupEngine():latest_backup_id_(0), on_running_(false){
                pthread_mutex_init(&(mutex_), NULL);
            }
            BackupID latest_backup_id_;
            std::atomic<bool> on_running_;
            pthread_mutex_t mutex_;

            std::map<std::string, rocksdb::BackupEngine*> engines_;
            std::map<std::string, BackupContent*> backup_content_;
            std::map<std::string, pthread_t> backup_pthread_ts_;
            
            inline std::string GetSaveDirByType(const std::string _dir, const std::string& _type) const {
                std::string backup_dir = _dir.empty() ? DEFAULT_BK_PATH : _dir;
                return backup_dir + ((backup_dir.back() != '/') ? "/" : "") + _type;
            }
            inline std::string GetRestoreDirByType(const std::string _dir, const std::string& _type) const {
                std::string restore_dir = _dir.empty() ? DEFAULT_RS_PATH : _dir;
                return restore_dir + ((restore_dir.back() != '/') ? "/" : "") + _type;
            }
            Status NewEngine(const BackupableOptions& options, const std::string &type);
            void WaitBackupPthread();
            void ClearBackupContent();
            Status InitLatestBackupID();
    };
}

#endif

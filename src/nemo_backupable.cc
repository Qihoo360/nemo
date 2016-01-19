#include <sys/types.h>
#include <dirent.h>
#include <string>
#include <vector>
#include <utility>
#include "nemo_backupable.h"
#include "xdebug.h"
#include "util.h"

namespace nemo {

    BackupEngine::~BackupEngine() {
        // Wait all children threads
        StopBackup();
        WaitBackupPthread();
        // Delete backup content
        ClearBackupContent();
        // Delete engines
        for (auto& engine : engines_) {
            delete engine.second;
        }
        pthread_mutex_destroy(&(mutex_));
    }
    
    Status BackupEngine::NewEngine(const BackupableOptions& _options, 
            const std::string& type) {
        std::string cur_dir = GetSaveDirByType(_options.backup_dir, type);
        if (opendir(cur_dir.c_str()) == NULL) {
            mkpath(cur_dir.c_str(), 0755);
        }

        rocksdb::BackupableDBOptions options(cur_dir);
        options.share_table_files = _options.share_table_files;
        options.sync = _options.sync;

        rocksdb::BackupEngine* backup_engine;
        rocksdb::Status s = rocksdb::BackupEngine::Open(rocksdb::Env::Default(), options, &backup_engine);
        if (!s.ok()) {
            log_warn("backup engine open failed, backup path: %s, open error %s", 
                    cur_dir.c_str(), s.ToString().c_str());
            return s;
        }

        if (!(engines_.insert(std::make_pair(type, backup_engine)).second)) {
            log_warn("backup engine open dupilicated, backup path: %s", cur_dir.c_str());
            delete backup_engine;
        }
        return s;
    }

    Status BackupEngine::Open(const BackupableOptions& _options,
                                  BackupEngine** backup_engine_ptr) { 
        *backup_engine_ptr = new BackupEngine();
        if (!*backup_engine_ptr){
            return Status::Corruption("New BackupEngine failed!");
        }

        // Create BackupEngine for each db type
        Status s;
        std::string types[] = {KV_DB, HASH_DB, LIST_DB, SET_DB, ZSET_DB};
        for (auto& type : types) {
            s = (*backup_engine_ptr)->NewEngine(_options, type);
            if (!s.ok()) {
                delete (*backup_engine_ptr);
                return s;
            }
        }
        
        s = (*backup_engine_ptr)->InitLatestBackupID();
        if (!s.ok()) {
            delete (*backup_engine_ptr);    
        }
        return s;
    }

    Status BackupEngine::SetBackupContent(nemo::Nemo *db) {
        {
            MutexLock l(&mutex_);
            if (on_running_) {
                return Status::Corruption("SetBackupContent process already exist!");  
            }
            on_running_ = true;
        }

        Status s;
        for (auto& engine : engines_) {
            BackupContent* bcontent = new BackupContent();
            // Erase item and log if alread exist
            std::map<std::string, BackupContent*>::iterator it = backup_content_.find(engine.first);
            if (it != backup_content_.end()) { //If already exists
                log_warn("backup_content get dupilicated, backup type: %s", it->first.c_str());
                delete it->second;
                backup_content_.erase(it);
            }

            //Get backup content
            rocksdb::DBWithTTL *tdb;
            if ((tdb = db->GetDBByType(engine.first)) == NULL) {
                log_warn("failed to find db by type: %s", engine.first.c_str());
            }
            s = engine.second->GetBackupFiles(tdb, true, 
                    bcontent->live_files, bcontent->live_wal_files, 
                    bcontent->manifest_file_size, bcontent->sequence_number);
            if (!s.ok()) {
                log_warn("get backup files faild for type: %s", engine.first.c_str());
                delete bcontent;
                {
                    MutexLock l(&mutex_);
                    on_running_ = false;
                }   
                return s;
            }
            backup_content_.insert(std::make_pair(engine.first, bcontent));
        }
        {
            MutexLock l(&mutex_);
            on_running_ = false;
        }   
        return s;
    }

    Status BackupEngine::CreateNewBackupSpecify(nemo::Nemo *db, const std::string &type) {
        rocksdb::DBWithTTL *tdb = db->GetDBByType(type);
        std::map<std::string, rocksdb::BackupEngine*>::iterator it_engine = engines_.find(type);
        std::map<std::string, BackupContent*>::iterator it_content = backup_content_.find(type);

        if (it_content != backup_content_.end() && 
                it_engine != engines_.end() &&
                tdb != NULL) {
            Status s = it_engine->second->CreateNewBackupWithFiles(
                    tdb, 
                    it_content->second->live_files, 
                    it_content->second->live_wal_files, 
                    it_content->second->manifest_file_size,
                    it_content->second->sequence_number);
            if (!s.ok()) {
                log_warn("backup engine create new failed, type: %s, error %s", 
                        type.c_str(), s.ToString().c_str());
                return s;
            }

        } else {
            log_warn("invalid db type: %s", type.c_str());
            return Status::Corruption("invalid db type");
        }
        return Status::OK();
    
    }

    void BackupEngine::PrintBackupContent() {
        for (auto& content : backup_content_) {
            log_info("backup content  type : %s", content.first.c_str());
        }
    }

    Status BackupEngine::RestoreFromBackupSpecify(const std::string &type, BackupID id, const std::string &db_dir) {
        std::map<std::string, rocksdb::BackupEngine*>::iterator it_engine = engines_.find(type);
        if (it_engine != engines_.end()) {
            std::string cur_dir = GetRestoreDirByType(db_dir, type);
            if (opendir(cur_dir.c_str()) == NULL) {
                mkpath(cur_dir.c_str(), 0755);
            }
            Status s = it_engine->second->RestoreDBFromBackup(id, cur_dir, cur_dir);
            if (!s.ok()) {
                log_warn("backup restore failed, backup_id %d, db path: %s, error %s", 
                        id, cur_dir.c_str(), s.ToString().c_str());
                return s;
            }
        } else {
            log_warn("invalid db type: %s", type.c_str());
            return Status::Corruption("invalid db type");
        }
        return Status::OK();
    }

    void* ThreadFuncSaveSpecify(void *arg) {
        BackupEngine* p = (BackupEngine*)(((BackupSaveArgs*)arg)->p_engine);
        Nemo *pdb = (Nemo*)(((BackupSaveArgs*)arg)->p_nemo);
        std::string key_type = ((BackupSaveArgs*)arg)->key_type;
        
        p->CreateNewBackupSpecify(pdb, key_type);
        pthread_exit(NULL);
    }

    void * ThreadFuncRestoreSpecify(void *arg) {
        BackupRestoreArgs* arg_ptr = (BackupRestoreArgs*)arg;
        BackupEngine* p = (BackupEngine*)(arg_ptr->p_engine);
        std::string key_type = arg_ptr->key_type;
        std::string db_dir = arg_ptr->db_dir;
        BackupID backup_id = arg_ptr->backup_id;

        p->RestoreFromBackupSpecify(key_type, backup_id, db_dir);
        pthread_exit(NULL);
    }

    void BackupEngine::WaitBackupPthread() { 
        int ret;
        for (auto& pthread : backup_pthread_ts_) {
            if ((ret = pthread_join(pthread.second, NULL)) != 0) {
                log_warn("pthread_join failed with backup thread for key_type: %s, error %d", 
                        pthread.first.c_str(), ret);
            }
        }
        backup_pthread_ts_.clear();
    }


    Status BackupEngine::CreateNewBackup(nemo::Nemo *db) {
        {
            MutexLock l(&mutex_);
            if (on_running_) {
                return Status::Corruption("Backup engine is busy now!");
            }
            on_running_ = true;
        }
        Status s = Status::OK();
        std::vector<BackupSaveArgs *> args;
        for (auto& engine : engines_) {
            pthread_t tid;
            BackupSaveArgs *arg = new BackupSaveArgs(db, (void*)this, engine.first);
            args.push_back(arg);
            if (pthread_create(&tid, NULL, &ThreadFuncSaveSpecify, arg) != 0) {
                s = Status::Corruption("pthead_create failed.");
                break;
            }
            if (!(backup_pthread_ts_.insert(std::make_pair(engine.first, tid)).second)) {
                log_warn("thread open dupilicated, type: %s", engine.first.c_str());
            }
        }
 
        // Wait threads stop
        if (!s.ok()) {
           StopBackup();
        }
        WaitBackupPthread();
        // Clear backup content
        ClearBackupContent();

        for (auto& a : args) {
            delete a;
        }
        {
            MutexLock l(&mutex_);
            on_running_ = false;
        }
        return Status::OK();
    }

    void BackupEngine::StopBackup() {
        for (auto& engine : engines_) {
            engine.second->StopBackup();  
        }
    }
    
    void BackupEngine::ClearBackupContent() {
        for (auto& content : backup_content_) {
            delete content.second;
        }
        backup_content_.clear();
    }
    
    Status BackupEngine::RestoreDBFromBackup(
                    BackupID backup_id, const std::string& db_dir) {
        {
            MutexLock l(&mutex_);
            if (on_running_) {
                return Status::Corruption("Backup engine is busy now!");
            }
            on_running_ = true;
        }
        std::vector<BackupRestoreArgs *> args;
        Status s = Status::OK();
        for (auto& engine : engines_) {
            pthread_t tid;
            BackupRestoreArgs *arg = new BackupRestoreArgs((void*)this, engine.first, 
                    backup_id, db_dir);
            args.push_back(arg);
            if (pthread_create(&tid, NULL, &ThreadFuncRestoreSpecify, arg) != 0) {
                s = Status::Corruption("pthead_create failed.");
                break;
            }
            if (!(backup_pthread_ts_.insert(std::make_pair(engine.first, tid)).second)) {
                log_warn("thread open dupilicated, type: %s", engine.first.c_str());
            }
        }
    
        if (!s.ok()) {
            StopBackup();
        }
        WaitBackupPthread();
        for (auto& a : args) {
            delete a;
        }
        {
            MutexLock l(&mutex_);
            on_running_ = false;
        }
        return s;
    }

    Status BackupEngine::InitLatestBackupID() {
        for (auto& engine : engines_){
            BackupID cur_id = engine.second->GetLatestBackupID();
            if (latest_backup_id_ != 0 && latest_backup_id_ != cur_id) {
                log_warn("latest_backup_id is not consistent, id is %d for type %s, while others is %d", 
                        cur_id, engine.first.c_str(), latest_backup_id_);
                return Status::Corruption("latest backup id is not consistent");
            }
            latest_backup_id_ = cur_id;
        }
        return Status::OK();
    }
}

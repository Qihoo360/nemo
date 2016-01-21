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
        // Delete engines
        for (auto& engine : engines_) {
            if (engine.second != NULL)
                delete engine.second;
            engine.second = NULL;
        }
        engines_.clear();
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
            //Get backup content
            BackupContent bcontent;
            rocksdb::DBWithTTL *tdb;
            if ((tdb = db->GetDBByType(engine.first)) == NULL) {
                log_warn("failed to find db by type: %s", engine.first.c_str());
            }
            VectorLogPtr vlptr;     //we don't need VectorLogPtr since we do flush before backup
            s = engine.second->GetBackupFiles(tdb, true, 
                    bcontent.live_files, vlptr, 
                    bcontent.manifest_file_size, bcontent.sequence_number);
            if (!s.ok()) {
                log_warn("get backup files faild for type: %s", engine.first.c_str());
                on_running_ = false;
                return s;
            }
            backup_content_[engine.first] = bcontent;
        }
        on_running_ = false;
        return s;
    }

    Status BackupEngine::CreateNewBackupSpecify(nemo::Nemo *db, const std::string &type) {
        rocksdb::DBWithTTL *tdb = db->GetDBByType(type);
        std::map<std::string, rocksdb::BackupEngine*>::iterator it_engine = engines_.find(type);
        std::map<std::string, BackupContent>::iterator it_content = backup_content_.find(type);

        VectorLogPtr vlptr;
        if (it_content != backup_content_.end() && 
                it_engine != engines_.end() &&
                tdb != NULL) {
            Status s = it_engine->second->CreateNewBackupWithFiles(
                    tdb, 
                    it_content->second.live_files, 
                    vlptr, 
                    it_content->second.manifest_file_size,
                    it_content->second.sequence_number);
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
        BackupSaveArgs* arg_ptr = static_cast<BackupSaveArgs*>(arg);
        BackupEngine* p = static_cast<BackupEngine*>(arg_ptr->p_engine);
        Nemo *pdb = static_cast<Nemo*>(arg_ptr->p_nemo);
        
        arg_ptr->res = p->CreateNewBackupSpecify(pdb, arg_ptr->key_type);

        pthread_exit(&(arg_ptr->res));
    }

    void* ThreadFuncRestoreSpecify(void *arg) {
        BackupRestoreArgs* arg_ptr = static_cast<BackupRestoreArgs*>(arg);
        BackupEngine* p = static_cast<BackupEngine*>(arg_ptr->p_engine);

        arg_ptr->res = p->RestoreFromBackupSpecify(arg_ptr->key_type, 
                arg_ptr->backup_id, arg_ptr->db_dir);
        
        pthread_exit(&(arg_ptr->res));
    }

    Status BackupEngine::WaitBackupPthread() { 
        int ret;
        Status s = Status::OK();
        for (auto& pthread : backup_pthread_ts_) {
            void *res;
            if ((ret = pthread_join(pthread.second, &res)) != 0) {
                log_warn("pthread_join failed with backup thread for key_type: %s, error %d", 
                        pthread.first.c_str(), ret);
            }
            Status cur_s = *(static_cast<Status*>(res));
            if (!cur_s.ok()) {
                log_warn("pthread executed failed with key_type: %s, error %s", 
                        pthread.first.c_str(), cur_s.ToString().c_str());
                StopBackup(); //stop others when someone failed
                s = cur_s;
            }
        }
        backup_pthread_ts_.clear();
        return s;
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
        std::vector<BackupSaveArgs*> args;
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
                backup_pthread_ts_[engine.first] = tid;
            }
        }
 
        // Wait threads stop
        if (!s.ok()) {
           StopBackup();
        }
        s = WaitBackupPthread();

        for (auto& a : args) {
            delete a;
        }
        on_running_ = false;
        return s;
    }

    void BackupEngine::StopBackup() {
        for (auto& engine : engines_) {
            if (engine.second != NULL)
                engine.second->StopBackup();  
        }
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
        Status s = Status::OK();
        std::vector<BackupRestoreArgs*> args;
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
                backup_pthread_ts_[engine.first] = tid;
            }
        }
    
        // Wait threads stop
        if (!s.ok()) {
            StopBackup();
        }
        s = WaitBackupPthread();

        for (auto& a : args) {
            delete a;
        }
        on_running_ = false;
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

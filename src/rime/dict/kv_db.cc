//
// Copyright RIME Developers
// Distributed under the BSD License
//
// 2014-12-04 Chen Gong <chen.sst@gmail.com>
//

#include <filesystem>
#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>
#include <rime/common.h>
#include <rime/service.h>
#include <rime/dict/kv_db.h>
#include <rime/dict/user_db.h>

namespace rime {

static const char* kMetaCharacter = "\x01";

struct KvDbCursor {
  rocksdb::Iterator* iterator = nullptr;

  KvDbCursor(rocksdb::DB* db) {
    rocksdb::ReadOptions options;
    options.fill_cache = false;
    iterator = db->NewIterator(options);
  }

  bool IsValid() const { return iterator && iterator->Valid(); }

  string GetKey() const { return iterator->key().ToString(); }

  string GetValue() const { return iterator->value().ToString(); }

  void Next() { iterator->Next(); }

  bool Jump(const string& key) {
    if (!iterator) {
      return false;
    }
    iterator->Seek(key);
    return true;
  }

  void Release() {
    delete iterator;
    iterator = nullptr;
  }
};

struct KvDbWrapper {
  rocksdb::DB* ptr = nullptr;
  rocksdb::WriteBatch batch;

  rocksdb::Status Open(const string& file_name, bool readonly) {
    rocksdb::Options options;
    options.create_if_missing = !readonly;
    return rocksdb::DB::Open(options, file_name, &ptr);
  }

  void Release() {
    delete ptr;
    ptr = nullptr;
  }

  KvDbCursor* CreateCursor() { return new KvDbCursor(ptr); }

  bool Fetch(const string& key, string* value) {
    auto status = ptr->Get(rocksdb::ReadOptions(), key, value);
    return status.ok();
  }

  bool Update(const string& key, const string& value, bool write_batch) {
    if (write_batch) {
      batch.Put(key, value);
      return true;
    }
    auto status = ptr->Put(rocksdb::WriteOptions(), key, value);
    return status.ok();
  }

  bool Erase(const string& key, bool write_batch) {
    if (write_batch) {
      batch.Delete(key);
      return true;
    }
    auto status = ptr->Delete(rocksdb::WriteOptions(), key);
    return status.ok();
  }

  void ClearBatch() { batch.Clear(); }

  bool CommitBatch() {
    auto status = ptr->Write(rocksdb::WriteOptions(), &batch);
    return status.ok();
  }
};

// KvDbAccessor members

KvDbAccessor::KvDbAccessor() {}

KvDbAccessor::KvDbAccessor(KvDbCursor* cursor, const string& prefix)
    : DbAccessor(prefix),
      cursor_(cursor),
      is_metadata_query_(prefix == kMetaCharacter) {
  Reset();
}

KvDbAccessor::~KvDbAccessor() {
  cursor_->Release();
}

bool KvDbAccessor::Reset() {
  return cursor_->Jump(prefix_);
}

bool KvDbAccessor::Jump(const string& key) {
  return cursor_->Jump(key);
}

bool KvDbAccessor::GetNextRecord(string* key, string* value) {
  if (!cursor_->IsValid() || !key || !value)
    return false;
  *key = cursor_->GetKey();
  if (!MatchesPrefix(*key)) {
    return false;
  }
  if (is_metadata_query_) {
    key->erase(0, 1);  // remove meta character
  }
  *value = cursor_->GetValue();
  cursor_->Next();
  return true;
}

bool KvDbAccessor::exhausted() {
  return !cursor_->IsValid() || !MatchesPrefix(cursor_->GetKey());
}

// KvDb members

KvDb::KvDb(const string& file_name,
           const string& db_name,
           const string& db_type)
    : Db(file_name, db_name), db_type_(db_type) {}

KvDb::~KvDb() {
  if (loaded())
    Close();
}

void KvDb::Initialize() {
  db_.reset(new KvDbWrapper);
}

an<DbAccessor> KvDb::QueryMetadata() {
  return Query(kMetaCharacter);
}

an<DbAccessor> KvDb::QueryAll() {
  an<DbAccessor> all = Query("");
  if (all)
    all->Jump(" ");  // skip metadata
  return all;
}

an<DbAccessor> KvDb::Query(const string& key) {
  if (!loaded())
    return nullptr;
  return New<KvDbAccessor>(db_->CreateCursor(), key);
}

bool KvDb::Fetch(const string& key, string* value) {
  if (!value || !loaded())
    return false;
  return db_->Fetch(key, value);
}

bool KvDb::Update(const string& key, const string& value) {
  if (!loaded() || readonly())
    return false;
  DLOG(INFO) << "update db entry: " << key << " => " << value;
  return db_->Update(key, value, in_transaction());
}

bool KvDb::Erase(const string& key) {
  if (!loaded() || readonly())
    return false;
  DLOG(INFO) << "erase db entry: " << key;
  return db_->Erase(key, in_transaction());
}

bool KvDb::Backup(const string& snapshot_file) {
  if (!loaded())
    return false;
  LOG(INFO) << "backing up db '" << name() << "' to " << snapshot_file;
  // TODO(chen): suppose we only use this method for user dbs.
  bool success = UserDbHelper(this).UniformBackup(snapshot_file);
  if (!success) {
    LOG(ERROR) << "failed to create snapshot file '" << snapshot_file
               << "' for db '" << name() << "'.";
  }
  return success;
}

bool KvDb::Restore(const string& snapshot_file) {
  if (!loaded() || readonly())
    return false;
  // TODO(chen): suppose we only use this method for user dbs.
  bool success = UserDbHelper(this).UniformRestore(snapshot_file);
  if (!success) {
    LOG(ERROR) << "failed to restore db '" << name() << "' from '"
               << snapshot_file << "'.";
  }
  return success;
}

bool KvDb::Recover() {
  LOG(INFO) << "trying to recover db '" << name() << "'.";
  auto status = rocksdb::RepairDB(file_name(), rocksdb::Options());
  if (status.ok()) {
    LOG(INFO) << "repair finished.";
    return true;
  }
  LOG(ERROR) << "db recovery failed: " << status.ToString();
  return false;
}

bool KvDb::Remove() {
  if (loaded()) {
    LOG(ERROR) << "attempt to remove opened db '" << name() << "'.";
    return false;
  }
  auto status = rocksdb::DestroyDB(file_name(), rocksdb::Options());
  if (!status.ok()) {
    LOG(ERROR) << "Error removing db '" << name() << "': " << status.ToString();
    return false;
  }
  return true;
}

bool KvDb::Open() {
  if (loaded())
    return false;
  Initialize();
  readonly_ = false;
  auto status = db_->Open(file_name(), readonly_);
  loaded_ = status.ok();

  if (loaded_) {
    string db_name;
    if (!MetaFetch("/db_name", &db_name)) {
      if (!CreateMetadata()) {
        LOG(ERROR) << "error creating metadata.";
        Close();
      }
    }
  } else {
    LOG(ERROR) << "Error opening db '" << name() << "': " << status.ToString();
  }
  return loaded_;
}

bool KvDb::OpenReadOnly() {
  if (loaded())
    return false;
  Initialize();
  readonly_ = true;
  auto status = db_->Open(file_name(), readonly_);
  loaded_ = status.ok();

  if (!loaded_) {
    LOG(ERROR) << "Error opening db '" << name() << "' read-only.";
  }
  return loaded_;
}

bool KvDb::Close() {
  if (!loaded())
    return false;

  db_->Release();

  LOG(INFO) << "closed db '" << name() << "'.";
  loaded_ = false;
  readonly_ = false;
  in_transaction_ = false;
  return true;
}

bool KvDb::CreateMetadata() {
  return Db::CreateMetadata() && MetaUpdate("/db_type", db_type_);
}

bool KvDb::MetaFetch(const string& key, string* value) {
  return Fetch(kMetaCharacter + key, value);
}

bool KvDb::MetaUpdate(const string& key, const string& value) {
  return Update(kMetaCharacter + key, value);
}

bool KvDb::BeginTransaction() {
  if (!loaded())
    return false;
  db_->ClearBatch();
  in_transaction_ = true;
  return true;
}

bool KvDb::AbortTransaction() {
  if (!loaded() || !in_transaction())
    return false;
  db_->ClearBatch();
  in_transaction_ = false;
  return true;
}

bool KvDb::CommitTransaction() {
  if (!loaded() || !in_transaction())
    return false;
  bool ok = db_->CommitBatch();
  db_->ClearBatch();
  in_transaction_ = false;
  return ok;
}

template <>
RIME_API string UserDbComponent<KvDb>::extension() const {
  return ".userdb";
}

template <>
RIME_API UserDbWrapper<KvDb>::UserDbWrapper(const string& file_name,
                                            const string& db_name)
    : KvDb(file_name, db_name, "userdb") {}

}  // namespace rime

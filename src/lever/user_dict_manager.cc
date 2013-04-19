//
// Copyleft 2012 RIME Developers
// License: GPLv3
//
// 2012-03-23 GONG Chen <chen.sst@gmail.com>
//
#include <fstream>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/foreach.hpp>
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/scope_exit.hpp>
#include <rime/common.h>
#include <rime/deployer.h>
#include <rime/algo/dynamics.h>
#include <rime/algo/utilities.h>
#include <rime/dict/db_utils.h>
#include <rime/dict/table_db.h>
#include <rime/dict/tree_db.h>
#include <rime/dict/user_db.h>
#include <rime/lever/user_dict_manager.h>

namespace fs = boost::filesystem;

namespace rime {

UserDictManager::UserDictManager(Deployer* deployer)
    : deployer_(deployer) {
  if (deployer) {
    path_ = deployer->user_data_dir;
  }
}

void UserDictManager::GetUserDictList(UserDictList* user_dict_list) {
  if (!user_dict_list) return;
  user_dict_list->clear();
  if (!fs::exists(path_) || !fs::is_directory(path_)) {
    LOG(INFO) << "directory '" << path_.string() << "' does not exist.";
    return;
  }
  fs::directory_iterator it(path_);
  fs::directory_iterator end;
  for (; it != end; ++it) {
    std::string name(it->path().filename().string());
    if (boost::ends_with(name, UserDb<TreeDb>::extension)) {
      boost::erase_last(name, UserDb<TreeDb>::extension);
      user_dict_list->push_back(name);
    }
  }
}

bool UserDictManager::Backup(const std::string& dict_name) {
  UserDb<TreeDb> db(dict_name);
  if (!db.OpenReadOnly())
    return false;
  if (db.GetUserId() != deployer_->user_id) {
    LOG(INFO) << "user id not match; recreating metadata in " << dict_name;
    if (!db.Close() || !db.Open() || !db.CreateMetadata()) {
      LOG(ERROR) << "failed to recreate metadata in " << dict_name;
      return false;
    }
  }
  boost::filesystem::path dir(deployer_->user_data_sync_dir());
  if (!boost::filesystem::exists(dir)) {
    if (!boost::filesystem::create_directories(dir)) {
      LOG(ERROR) << "error creating directory '" << dir.string() << "'.";
      return false;
    }
  }
  std::string snapshot_file =
      dict_name + UserDb<TreeDb>::extension + ".snapshot";
  return db.Backup((dir / snapshot_file).string());
}

bool UserDictManager::Restore(const std::string& snapshot_file) {
  UserDb<TreeDb> temp(".temp");
  if (temp.Exists())
    temp.Remove();
  if (!temp.Open())
    return false;
  BOOST_SCOPE_EXIT( (&temp) )
  {
    temp.Close();
    temp.Remove();
  } BOOST_SCOPE_EXIT_END
  if (!temp.Restore(snapshot_file))
    return false;
  if (!temp.IsUserDb())
    return false;
  std::string db_name(temp.GetDbName());
  if (db_name.empty())
    return false;
  UserDb<TreeDb> dest(db_name);
  if (!dest.Open())
    return false;
  BOOST_SCOPE_EXIT( (&dest) )
  {
    dest.Close();
  } BOOST_SCOPE_EXIT_END
  LOG(INFO) << "merging '" << snapshot_file
            << "' from " << temp.GetUserId()
            << " into userdb '" << db_name << "'...";
  TickCount tick_left = dest.GetTickCount();
  TickCount tick_right = temp.GetTickCount();
  TickCount tick_max = (std::max)(tick_left, tick_right);
  shared_ptr<DbAccessor> a = temp.Query("");
  a->Jump(" ");  // skip metadata
  std::string key, left, right;
  int num_entries = 0;
  while (a->GetNextRecord(&key, &right)) {
    size_t tab_pos = key.find('\t');
    if (tab_pos == 0 || tab_pos == std::string::npos)
      continue;
    // fix invalid keys created by a buggy version of Import()
    if (key[tab_pos - 1] != ' ')
      key.insert(tab_pos, 1, ' ');
    UserDbValue v(right);
    if (v.tick < tick_right)
      v.dee = algo::formula_d(0, (double)tick_right, v.dee, (double)v.tick);
    if (dest.Fetch(key, &left)) {
      UserDbValue u(left);
      if (u.tick < tick_left)
        u.dee = algo::formula_d(0, (double)tick_left, u.dee, (double)u.tick);
      v.commits = (std::max)(v.commits, u.commits);
      v.dee = (std::max)(v.dee, u.dee);
    }
    v.tick = tick_max;
    if (dest.Update(key, v.Pack()))
      ++num_entries;
  }
  if (num_entries > 0) {
    try {
      dest.MetaUpdate("/tick", boost::lexical_cast<std::string>(tick_max));
      dest.MetaUpdate("/user_id", deployer_->user_id);
    }
    catch (...) {
      LOG(WARNING) << "failed to update tick count.";
    }
  }
  LOG(INFO) << "total " << num_entries << " entries imported, tick = "
            << tick_max;
  return true;
}

int UserDictManager::Export(const std::string& dict_name,
                            const std::string& text_file) {
  UserDb<TreeDb> db(dict_name);
  if (!db.OpenReadOnly())
    return -1;
  BOOST_SCOPE_EXIT( (&db) )
  {
    db.Close();
  } BOOST_SCOPE_EXIT_END
  if (!db.IsUserDb())
    return -1;
  DbSource source(&db);
  source.file_description = "Rime user dictionary export";
  TsvWriter writer(&source, TableDb::format.formatter);
  int num_entries = 0;
  try {
    num_entries = writer(text_file);
    DLOG(INFO) << num_entries << " entries saved.";
  }
  catch (std::exception& ex) {
    LOG(ERROR) << ex.what();
    return -1;
  }
  return num_entries;
}

int UserDictManager::Import(const std::string& dict_name,
                            const std::string& text_file) {
  UserDb<TreeDb> db(dict_name);
  if (!db.Open())
    return -1;
  BOOST_SCOPE_EXIT( (&db) )
  {
    db.Close();
  } BOOST_SCOPE_EXIT_END
  if (!db.IsUserDb())
    return -1;
  std::ifstream fin(text_file.c_str());
  std::string line, key, value;
  int num_entries = 0;
  while (getline(fin, line)) {
    // skip empty lines and comments
    if (line.empty() || line[0] == '#') continue;
    // read a dict entry
    std::vector<std::string> row;
    boost::algorithm::split(row, line,
                            boost::algorithm::is_any_of("\t"));
    if (row.size() < 2 ||
        row[0].empty() || row[1].empty()) {
      LOG(WARNING) << "invalid entry at #" << num_entries << ".";
      continue;
    }
    boost::algorithm::trim(row[1]);
    if (!row[1].empty()) {
      std::vector<std::string> syllables;
      boost::algorithm::split(syllables, row[1],
                              boost::algorithm::is_any_of(" "),
                              boost::algorithm::token_compress_on);
      row[1] = boost::algorithm::join(syllables, " ");
    }
    key = row[1] + " \t" + row[0];
    int commits = 0;
    if (row.size() >= 3 && !row[2].empty()) {
      try {
        commits = boost::lexical_cast<int>(row[2]);
      }
      catch (...) {
      }
    }
    UserDbValue v;
    if (db.Fetch(key, &value))
      v.Unpack(value);
    if (commits > 0)
      v.commits = (std::max)(commits, v.commits);
    else if (commits < 0)  // mark as deleted
      v.commits = commits;
    if (db.Update(key, v.Pack()))
      ++num_entries;
  }
  fin.close();
  return num_entries;
}

bool UserDictManager::UpgradeUserDict(const std::string& dict_name) {
  UserDb<TreeDb> db(dict_name);
  if (!db.OpenReadOnly())
    return false;
  if (!db.IsUserDb())
    return false;
  std::string db_creator_version(db.GetRimeVersion());
  if (CompareVersionString(db_creator_version, "0.9.7") < 0) {
    // fix invalid keys created by a buggy version of Import()
    LOG(INFO) << "upgrading user dict '" << dict_name << "'.";
    fs::path trash = fs::path(deployer_->user_data_dir) / "trash";
    if (!fs::exists(trash)) {
      boost::system::error_code ec;
      if (!fs::create_directories(trash, ec)) {
        LOG(ERROR) << "error creating directory '" << trash.string() << "'.";
        return false;
      }
    }
    fs::path snapshot_file = trash / (db.name() + ".snapshot");
    return db.Backup(snapshot_file.string()) &&
        db.Close() &&
        db.Remove() &&
        Restore(snapshot_file.string());
  }
  return true;
}

bool UserDictManager::Synchronize(const std::string& dict_name) {
  LOG(INFO) << "synchronize user dict '" << dict_name << "'.";
  bool success = true;
  fs::path sync_dir(deployer_->sync_dir);
  if (!fs::exists(sync_dir)) {
    boost::system::error_code ec;
    if (!fs::create_directories(sync_dir, ec)) {
      LOG(ERROR) << "error creating directory '" << sync_dir.string() << "'.";
      return false;
    }
  }
  fs::directory_iterator it(sync_dir);
  fs::directory_iterator end;
  std::string snapshot_file =
      dict_name + UserDb<TreeDb>::extension + ".snapshot";
  for (; it != end; ++it) {
    if (!fs::is_directory(it->path()))
      continue;
    fs::path file_path = it->path() / snapshot_file;
    if (fs::exists(file_path)) {
      LOG(INFO) << "merging snapshot file: " << file_path.string();
      if (!Restore(file_path.string())) {
        LOG(ERROR) << "failed to merge snapshot file: " << file_path.string();
        success = false;
      }
    }
  }
  if (!Backup(dict_name)) {
    LOG(ERROR) << "error backing up user dict '" << dict_name << "'.";
    success = false;
  }
  return success;
}

bool UserDictManager::SynchronizeAll() {
  UserDictList user_dicts;
  GetUserDictList(&user_dicts);
  LOG(INFO) << "synchronizing " << user_dicts.size() << " user dicts.";
  BOOST_FOREACH(const std::string& dict_name, user_dicts) {
    if (!Synchronize(dict_name))
      return false;
  }
  return true;
}

}  // namespace rime

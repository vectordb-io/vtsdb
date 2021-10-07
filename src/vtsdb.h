#ifndef __VTSDB_H__
#define __VTSDB_H__

#include <cstdio>
#include <set>
#include <map>
#include <vector>
#include <string>
#include <iostream>
#include <leveldb/db.h>
#include "vtsdb.pb.h"

namespace vectordb {

class VTsdb {

//#define ALL_TAGS_KEY "ALL_TAGS_KEY"

  public:
    VTsdb(const std::string& path);
    ~VTsdb();
    VTsdb(const VTsdb&) = delete;
    VTsdb& operator=(const VTsdb&) = delete;

    std::string meta_path() const {
        return path_ + "/meta";
    }

    std::string tag_path() const {
        return path_ + "/tags";
    }

    std::string column_path(const std::string& column_name) const {
        return path_ + "/column_" + column_name;
    }

    size_t ColumnNum() const {
        return column_names_.size();
    }

    bool Load();
    bool CreateTable(const std::vector<std::string> &column_names);
    bool Insert(uint64_t timestamp, const std::vector<std::string> &values, const std::vector<std::string> &tags);
    bool Insert(const std::string &timestr, const std::vector<std::string> &values, const std::vector<std::string> &tags);
    bool Insert(const std::vector<std::string> &values, const std::vector<std::string> &tags);
    bool Get(const std::string &time_begin, const std::string &time_end, std::vector<std::vector<std::string>> &values) const;
    bool Get(uint64_t time_begin, uint64_t time_end, std::vector<std::vector<std::string>> &values) const;
    bool Get(const std::vector<std::string> filter_tags, std::vector<std::vector<std::string>> &values) const;
    bool Dump() const;

  private:
    bool CreateTable2(const std::vector<std::string> &column_names);
    bool Get(const std::string &key, std::vector<std::string> &values) const;

    std::vector<std::string> tag_names_;
    std::vector<std::string> column_names_; // first column name is "timestamp", value is tags
    std::string path_;
    leveldb::DB* db_meta_;
    std::map<std::string, leveldb::DB*> db_data_;
    leveldb::DB* db_tags_; // all tag names, tag reverse index
};

} // namespace vectordb

#endif

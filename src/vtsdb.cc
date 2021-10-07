#include "util.h"
#include "vtsdb.h"

namespace vectordb {

VTsdb::VTsdb(const std::string& path)
    :path_(path) {
}

VTsdb::~VTsdb() {
    delete db_meta_;
    for (auto &kv : db_data_) {
        leveldb::DB *db = kv.second;
        delete db;
    }
    delete db_tags_;
}

bool
VTsdb::Load() {

    return true;
}

bool
VTsdb::CreateTable(const std::vector<std::string> &column_names) {
    std::vector<std::string> column_names2;
    column_names2.push_back("timestamp");
    for (auto &v : column_names) {
        column_names2.push_back(v);
    }
    auto b = CreateTable2(column_names2);
    return b;
}

bool
VTsdb::CreateTable2(const std::vector<std::string> &column_names) {
    if (column_names.size() < 1) {
        std::cout << "column_names error" << std::endl;
        return false;
    }

    std::set<std::string> column_set;
    for (auto &name : column_names) {
        column_set.insert(name);
    }

    if (column_names.size() != column_set.size()) {
        std::cout << "duplicate column" << std::endl;
        return false;
    }

    auto b = util::RecurMakeDir2(path_);
    assert(b);

    leveldb::Options options;
    options.create_if_missing = true;
    options.error_if_exists = true;

    auto s = leveldb::DB::Open(options, meta_path(), &db_meta_);
    assert(s.ok());

    s = leveldb::DB::Open(options, tag_path(), &db_tags_);
    assert(s.ok());

    /*
    vtsdb::Strings pb;
    std::string all_tags;
    pb.SerializeToString(&all_tags);
    s = db_tags_->Put(leveldb::WriteOptions(), ALL_TAGS_KEY, all_tags);
    assert(s.ok());
    */

    for (auto &name : column_names) {
        leveldb::DB* db = nullptr;
        auto s = leveldb::DB::Open(options, column_path(name), &db);
        assert(s.ok());

        column_names_.push_back(name);
        db_data_.insert(std::pair<std::string, leveldb::DB*>(name, db));
    }

    return true;
}

bool
VTsdb::Insert(uint64_t timestamp, const std::vector<std::string> &values, const std::vector<std::string> &tags) {
    if (values.size() != ColumnNum() - 1) {
        std::cout << "ColumnNum error" << std::endl;
        return false;
    }

    std::string key;
    util::Timestamp2KeyString(timestamp, key);

    for (int i = 0; i < ColumnNum(); ++i) {
        std::string column_name = column_names_[i];

        std::string value;
        if (column_name == "timestamp") {
            assert(i == 0);

            vtsdb::Strings pb;
            for (auto &tag : tags) {
                pb.add_strings(tag);
            }
            pb.SerializeToString(&value);

        } else {
            value = values[i - 1];
        }

        auto it = db_data_.find(column_name);
        assert(it != db_data_.end());

        leveldb::DB* db = it->second;
        auto s = db->Put(leveldb::WriteOptions(), key, value);
        assert(s.ok());
    }

    for (auto &tag : tags) {
        std::string value;
        auto s = db_tags_->Get(leveldb::ReadOptions(), tag, &value);
        if (s.IsNotFound()) {
            vtsdb::Strings pb;
            pb.add_strings(key);
            pb.SerializeToString(&value);
            s = db_tags_->Put(leveldb::WriteOptions(), tag, value);
            assert(s.ok());

        } else if (s.ok()) {
            vtsdb::Strings pb;
            pb.ParseFromString(value);
            pb.add_strings(key);
            pb.SerializeToString(&value);
            s = db_tags_->Put(leveldb::WriteOptions(), tag, value);
            assert(s.ok());

        } else {
            assert(0);
        }
    }

    return true;
}

// "2021-10-06 20:04:12"
bool
VTsdb::Insert(const std::string &timestr, const std::vector<std::string> &values, const std::vector<std::string> &tags) {
    uint64_t timestamp;
    auto b = util::String2TimeStamp(timestr, timestamp);
    assert(b);

    b = Insert(timestamp, values, tags);
    return b;
}

bool
VTsdb::Insert(const std::vector<std::string> &values, const std::vector<std::string> &tags) {
    bool b = Insert(time(nullptr), values, tags);
    return b;
}

bool
VTsdb::Get(const std::string &time_begin, const std::string &time_end, std::vector<std::vector<std::string>> &values) const {
    uint64_t tb, te;

    auto b = util::String2TimeStamp(time_begin, tb);
    assert(b);
    b = util::String2TimeStamp(time_end, te);
    assert(b);

    b = Get(tb, te, values);
    return b;
}

bool
VTsdb::Get(const std::string &key, std::vector<std::string> &values) const {
    values.clear();
    values.push_back(key);

    uint64_t timestamp;
    auto b = util::KeyString2Timestamp(key, timestamp);
    assert(b);

    std::string ts;
    util::TimeStamp2String(timestamp, ts);
    values.push_back(ts);

    for (auto &name : column_names_) {
        auto it = db_data_.find(name);
        assert(it != db_data_.end());
        leveldb::DB* db = it->second;

        std::string value;
        auto s = db->Get(leveldb::ReadOptions(), key, &value);
        assert(s.ok());

        if (name == "timestamp") {
            vtsdb::Strings pb;
            pb.ParseFromString(value);
            value.clear();
            for (int i = 0; i < pb.strings_size(); ++i) {
                value.append(pb.strings(i));
                value.append("#");
            }
        }

        values.push_back(value);
    }

    return true;
}

bool
VTsdb::Get(uint64_t time_begin, uint64_t time_end, std::vector<std::vector<std::string>> &values) const {
    values.clear();

    auto it = db_data_.find("timestamp");
    assert(it != db_data_.end());
    leveldb::DB* db = it->second;

    std::string begin_key, end_key;
    auto b = util::Timestamp2KeyString(time_begin, begin_key);
    assert(b);
    b = util::Timestamp2KeyString(time_end, end_key);
    assert(b);

    std::vector<std::string> get_keys;
    leveldb::Iterator* db_it = db->NewIterator(leveldb::ReadOptions());
    for (db_it->Seek(begin_key);
            db_it->Valid() && db_it->key().ToString() <= end_key;
            db_it->Next()) {
        get_keys.push_back(db_it->key().ToString());
    }

    for (auto &k : get_keys) {
        std::vector<std::string> one_values;

        auto b = Get(k, one_values);
        assert(b);

        values.push_back(one_values);
    }

    assert(db_it->status().ok());  // Check for any errors found during the scan
    delete db_it;

    return true;
}

bool
VTsdb::Get(const std::vector<std::string> filter_tags, std::vector<std::vector<std::string>> &values) const {
    std::map<std::string, int> m;

    for (auto &tag : filter_tags) {
        std::string value;
        auto s = db_tags_->Get(leveldb::ReadOptions(), tag, &value);
        assert(s.ok());

        vtsdb::Strings pb;
        pb.ParseFromString(value);
        std::vector<std::string> get_keys;
        for (int i = 0; i < pb.strings_size(); ++i) {
            get_keys.push_back(pb.strings(i));
        }

        for (auto &key : get_keys) {
            auto it = m.find(key);
            if (it == m.end()) {
                m.insert(std::pair<std::string, int>(key, 1));
            } else {
                (it->second)++;
            }
        }
    }

    for (auto &kv : m) {
        if (kv.second == filter_tags.size()) {
            std::string key = kv.first;

            std::vector<std::string> one_values;
            auto b = Get(key, one_values);
            assert(b);
            values.push_back(one_values);
        }
    }

    return true;
}

bool
VTsdb::Dump() const {
    uint64_t time_begin, time_end;
    time_begin = 0;
    time_end = 0xFFFFFFFFFFFFFFFF;

    std::vector<std::vector<std::string>> values;
    auto b = Get(time_begin, time_end, values);
    if (b) {
        for (auto &vec : values) {
            for (auto &value : vec) {
                std::cout << value << ", ";
            }
            std::cout << std::endl;
        }
    }

    return b;
}


} // namespace vectordb

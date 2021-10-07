#include <iostream>
#include "vtsdb.h"
#include "util.h"

int
main(int argc, char **argv) {
    vectordb::VTsdb vt("./test_vtsdb");

    std::vector<std::string> column_names;
    column_names.push_back("name");
    column_names.push_back("country");
    column_names.push_back("city");
    auto b = vt.CreateTable(column_names);
    assert(b);

    {
        std::vector<std::string> values;
        values.push_back("Baidu");
        values.push_back("China");
        values.push_back("BeiJing");

        std::vector<std::string> tags;
        tags.push_back("LiYanHong");
        tags.push_back("search");
        tags.push_back("ai");

        auto b = vt.Insert(std::string("2000-01-01 09:00:00"), values, tags);
        assert(b);
    }

    {
        std::vector<std::string> values;
        values.push_back("Alibaba");
        values.push_back("China");
        values.push_back("HangZhou");

        std::vector<std::string> tags;
        tags.push_back("MaYun");
        tags.push_back("business");

        auto b = vt.Insert(std::string("1999-01-01 09:00:00"), values, tags);
        assert(b);
    }

    {
        std::vector<std::string> values;
        values.push_back("Tencent");
        values.push_back("China");
        values.push_back("ShenZhen");

        std::vector<std::string> tags;
        tags.push_back("MaHuaTeng");
        tags.push_back("social");

        auto b = vt.Insert(std::string("1998-11-01 09:00:00"), values, tags);
        assert(b);
    }

    {
        std::vector<std::string> values;
        values.push_back("Google");
        values.push_back("America");
        values.push_back("Mountain");

        std::vector<std::string> tags;
        tags.push_back("Page");
        tags.push_back("Brin");
        tags.push_back("search");
        tags.push_back("ai");

        auto b = vt.Insert(std::string("1998-09-04 09:00:00"), values, tags);
        assert(b);
    }

    {
        std::vector<std::string> values;
        values.push_back("Amazon");
        values.push_back("America");
        values.push_back("Seattle");

        std::vector<std::string> tags;
        tags.push_back("Bezos");
        tags.push_back("business");

        auto b = vt.Insert(std::string("1994-01-01 09:00:00"), values, tags);
        assert(b);
    }

    {
        std::vector<std::string> values;
        values.push_back("Facebook");
        values.push_back("America");
        values.push_back("MenloPark");

        std::vector<std::string> tags;
        tags.push_back("Zuckerberg");
        tags.push_back("social");
        tags.push_back("ai");

        auto b = vt.Insert(std::string("2004-02-04 09:00:00"), values, tags);
        assert(b);
    }

    vt.Dump();
    std::cout << std::endl;

    {
        std::vector<std::string> filter_tags;
        filter_tags.push_back("social");
        std::vector<std::vector<std::string>> values;
        auto b = vt.Get(filter_tags, values);
        assert(b);

        for (auto &vec : values) {
            for (auto &value : vec) {
                std::cout << value << ", ";
            }
            std::cout << std::endl;
        }
        std::cout << std::endl;
    }

    {
        std::vector<std::string> filter_tags;
        filter_tags.push_back("ai");
        std::vector<std::vector<std::string>> values;
        auto b = vt.Get(filter_tags, values);
        assert(b);

        for (auto &vec : values) {
            for (auto &value : vec) {
                std::cout << value << ", ";
            }
            std::cout << std::endl;
        }
        std::cout << std::endl;
    }

    {
        std::vector<std::string> filter_tags;
        filter_tags.push_back("ai");
        filter_tags.push_back("search");
        std::vector<std::vector<std::string>> values;
        auto b = vt.Get(filter_tags, values);
        assert(b);

        for (auto &vec : values) {
            for (auto &value : vec) {
                std::cout << value << ", ";
            }
            std::cout << std::endl;
        }
        std::cout << std::endl;
    }

    {
        std::vector<std::vector<std::string>> values;
        b = vt.Get("1998-01-01 00:00:00", "1999-12-12 23:59:59", values);
        assert(b);

        for (auto &vec : values) {
            for (auto &value : vec) {
                std::cout << value << ", ";
            }
            std::cout << std::endl;
        }
        std::cout << std::endl;
    }

    return 0;
}

#define CATCH_CONFIG_MAIN

#include "LWWElementDict.h"
#include <chrono>
#include <thread>
#include <vector>
#include <ctime>
#include <string>


typedef std::chrono::system_clock::time_point Timestamp;


std::ostream & operator<<(std::ostream & os, const std::pair<char, std::pair<int, Timestamp>> & element) {
    time_t t = std::chrono::system_clock::to_time_t(element.second.second);
    std::string elementToString = " { " + std::string(1, element.first) + " , { "
        + std::to_string(element.second.first) + " , " + ctime(&t) + " } } ";
    os << elementToString;
    return os;
}


#include "catch.hpp"


TEST_CASE("Multiple inserts - time chronologically") {
    char c = 'A';

    int i1 = 10;
    int i2 = 20;

    Timestamp t1 = std::chrono::system_clock::now();
    Timestamp t2 = t1 + std::chrono::minutes(4);


    LWWElementDict<char, int, Timestamp> dict;
    dict.addElement(c, i1, t1);
    dict.addElement(c, i2, t2);

    REQUIRE(dict.getValueByKey(c) == 20);
}


TEST_CASE("Multiple inserts - time non chronologically") {
    char c = 'A';

    int i1 = 10;
    int i2 = 20;

    Timestamp t1 = std::chrono::system_clock::now();
    Timestamp t2 = t1 + std::chrono::minutes(4);


    LWWElementDict<char, int, Timestamp> dict;
    dict.addElement(c, i2, t2);
    dict.addElement(c, i1, t1);

    REQUIRE(dict.getValueByKey(c) == 10);
}


TEST_CASE("Element removal - time chronologically") {
    char c = 'A';

    int i = 10;

    Timestamp t1 = std::chrono::system_clock::now();
    Timestamp t2 = t1 + std::chrono::minutes(4);


    LWWElementDict<char, int, Timestamp> dict;
    dict.addElement(c, i, t1);
    dict.removeElement(c, i, t2);

    REQUIRE(dict.getValueByKey(c).has_value() == false);
}


TEST_CASE("Element removal - time non chronologically") {
    char c = 'A';

    int i = 10;

    Timestamp t1 = std::chrono::system_clock::now();
    Timestamp t2 = t1 + std::chrono::minutes(4);


    LWWElementDict<char, int, Timestamp> dict;
    dict.addElement(c, i, t2);
    dict.removeElement(c, i, t1);

    REQUIRE(dict.getValueByKey(c).has_value() == true);
}


TEST_CASE("Element removal - time concurrent") {
    char c = 'A';

    int i = 10;

    Timestamp t = std::chrono::system_clock::now();


    LWWElementDict<char, int, Timestamp> dict;
    dict.addElement(c, i, t);
    dict.removeElement(c, i, t);

    REQUIRE(dict.getValueByKey(c).has_value() == false);
}


TEST_CASE("Element removal - time concurrent, removal first") {
    char c = 'A';

    int i = 10;

    Timestamp t = std::chrono::system_clock::now();


    LWWElementDict<char, int, Timestamp> dict;
    dict.removeElement(c, i, t);
    dict.addElement(c, i, t);

    REQUIRE(dict.getValueByKey(c).has_value() == false);
}


TEST_CASE("Testing data merge") {
    char c1 = 'A';
    char c2 = 'B';

    int i1 = 10;
    int i2 = 20;

    Timestamp t1 = std::chrono::system_clock::now();
    Timestamp t2 = t1 + std::chrono::minutes(4);


    LWWElementDict<char, int, Timestamp> dict1;
    dict1.addElement(c1, i1, t1);
    dict1.addElement(c1, i2, t2);
    dict1.addElement(c1, i1, t2);
    dict1.addElement(c2, i1, t1);
    dict1.addElement(c2, i1, t2);
    dict1.addElement(c2, i2, t1);
    dict1.addElement(c2, i2, t2);

    LWWElementDict<char, int, Timestamp> dict2;
    dict2.addElement(c1, i2, t1);
    dict2.addElement(c2, i1, t2);
    dict2.addElement(c2, i2, t1);

    dict2.mergeWith(dict1);

    std::vector<std::pair<char, std::pair<int, Timestamp>>> mergeExpected = {
        { c1, { i1, t1 } },
        { c1, { i1, t2 } },
        { c1, { i2, t1 } },
        { c1, { i2, t2 } },
        { c2, { i1, t1 } },
        { c2, { i1, t2 } },
        { c2, { i2, t1 } },
        { c2, { i2, t2 } }
    };
    std::vector<std::pair<char, std::pair<int, Timestamp>>> mergeResult;

    const auto addedMergedData = dict2.getAddedData();
    for(const auto & [key, multimap] : addedMergedData) {
        for(auto multimapIter = multimap.begin(); multimapIter != multimap.end(); ++multimapIter) {
            mergeResult.push_back({ key, *multimapIter });
        }
    }

    REQUIRE(dict2.getValueByKey(c1) == i2);
    REQUIRE(dict2.getValueByKey(c2) == i1);
    REQUIRE(mergeExpected == mergeResult);
}

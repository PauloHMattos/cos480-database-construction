// DatabaseSystem.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include <iostream>
#include "pch.h"
#include "FixedRecord.h"
#include "../DatabaseSystem.Core/Table.h"
#include "../DatabaseSystems.Heap/HeapRecordManager.h"

#define SPANOF(value) span<unsigned char>((unsigned char*)&value, sizeof(value))

void runBenchmark(Table& table);
void insertMany(Table& table, vector<Record> records);
void findOne(Table& table);
void findAllSet(Table& table);
void findAllBetween(Table& table);
void findAllEquals(Table& table);
void deleteOne(Table& table);
void deleteAllEquals(Table& table);
void printRecords(vector<Record*> records, string label = "Records");

int main()
{
    auto fixedSchema = FixedRecord::CreateSchema();

    auto dbPath = ".\\test.db";
    auto heap = HeapRecordManager(4096, 0.001);
    auto table = Table(heap);
    //table.Load(dbPath);
    table.Create(dbPath, fixedSchema);
    auto records = Record::LoadFromCsv(*fixedSchema, ".\\cbd.csv", -1);

    insertMany(table, records);
    /*
    findOne(table);
    findAllSet(table);
    findAllBetween(table);
    findAllEquals(table);
    /*/

    //*
    deleteAllEquals(table);
    findAllEquals(table);
    insertMany(table, records);
    //*/

    table.Close();
}

void insertMany(Table& table, vector<Record> records)
{
    cout << "[InsertMany]" << endl;
    cout << "- Count = " << records.size() << endl;
    auto start = std::chrono::high_resolution_clock::now();
    table.InsertMany(records);
    auto finish = std::chrono::high_resolution_clock::now();
    auto microseconds = std::chrono::duration_cast<std::chrono::milliseconds>(finish - start);
    cout << "- Duration = " << microseconds.count() << " ms" << endl;
    cout << "- Space usage = " << table.GetSize() << " Bytes" << endl;
    cout << "- Read Blocks = " << table.GetLastQueryBlockReadAccessCount() << endl;
    cout << "- Write Blocks = " << table.GetLastQueryBlockWriteAccessCount() << endl;
    cout << endl;
}

void findOne(Table& table)
{
    cout << "[Find] Random Id" << endl;

    std::random_device rd; // obtain a random number from hardware
    std::mt19937 gen(rd()); // seed the generator
    std::uniform_int_distribution<unsigned long long> distr(0, 100000); // define the range

    auto id = distr(gen);
    cout << "- Id = " << id << endl;

    auto start = std::chrono::high_resolution_clock::now();
    auto record = table.Select(id);
    auto finish = std::chrono::high_resolution_clock::now();
    auto microseconds = std::chrono::duration_cast<std::chrono::milliseconds>(finish - start);
    cout << "- Duration = " << microseconds.count() << " ms" << endl;
    cout << "- Read Blocks = " << table.GetLastQueryBlockReadAccessCount() << endl;
    cout << "- Write Blocks = " << table.GetLastQueryBlockWriteAccessCount() << endl;
    cout << "- Result = ";
    if (record != nullptr)
    {
        record->Write(cout);
    }
    else {
        cout << " Not Found";
    }
    cout << endl;
    cout << endl;
}


void findAllSet(Table& table)
{
    cout << "[FindAll] Random Set" << endl;

    std::random_device rd; // obtain a random number from hardware
    std::mt19937 gen(rd()); // seed the generator
    std::uniform_int_distribution<unsigned long long> distr(0, 100000); // define the range
    
    cout << "- Ids = [";
    int count = 10;
    auto ids = vector<unsigned long long>();
    for (int i = 0; i < count; i++)
    {
        auto id = distr(gen);
        cout << id << ", ";
        ids.push_back(id);
    }
    cout << '\b' << '\b' << "]" << endl;

    auto start = std::chrono::high_resolution_clock::now();
    auto records = table.Select(ids);
    auto finish = std::chrono::high_resolution_clock::now();
    auto microseconds = std::chrono::duration_cast<std::chrono::milliseconds>(finish - start);
    cout << "- Duration = " << microseconds.count() << " ms" << endl;
    cout << "- Read Blocks = " << table.GetLastQueryBlockReadAccessCount() << endl;
    cout << "- Write Blocks = " << table.GetLastQueryBlockWriteAccessCount() << endl;
    printRecords(records);
    cout << endl;
}

void findAllBetween(Table& table)
{
    float min = 117;
    float max = 200;
    cout << "[FindAll] Weigth Between " << min << " - " << max << endl;

    auto start = std::chrono::high_resolution_clock::now();
    auto records = table.SelectWhereBetween(NAMEOF(FixedRecord::Weigth).str(), SPANOF(min), SPANOF(max));
    auto finish = std::chrono::high_resolution_clock::now();
    auto microseconds = std::chrono::duration_cast<std::chrono::milliseconds>(finish - start);
    cout << "- Duration = " << microseconds.count() << " ms" << endl;
    cout << "- Read Blocks = " << table.GetLastQueryBlockReadAccessCount() << endl;
    cout << "- Write Blocks = " << table.GetLastQueryBlockWriteAccessCount() << endl;
    printRecords(records);
    cout << endl;
}

void findAllEquals(Table& table)
{
    char city[40] = "Taguatinga";
    cout << "[FindAll] City = " << city << endl;

    auto start = std::chrono::high_resolution_clock::now();
    auto records = table.SelectWhereEquals(NAMEOF(FixedRecord::City).str(), SPANOF(city));
    auto finish = std::chrono::high_resolution_clock::now();
    auto microseconds = std::chrono::duration_cast<std::chrono::milliseconds>(finish - start);
    cout << "- Duration = " << microseconds.count() << " ms" << endl;
    cout << "- Read Blocks = " << table.GetLastQueryBlockReadAccessCount() << endl;
    cout << "- Write Blocks = " << table.GetLastQueryBlockWriteAccessCount() << endl;
    printRecords(records);
    cout << endl;
}

void deleteOne(Table& table)
{

}

void deleteAllEquals(Table& table)
{
    char city[40] = "Taguatinga";
    cout << "[DeleteAll] City = " << city << endl;

    auto start = std::chrono::high_resolution_clock::now();
    auto records = table.DeleteWhereEquals(NAMEOF(FixedRecord::City).str(), SPANOF(city));
    auto finish = std::chrono::high_resolution_clock::now();
    auto microseconds = std::chrono::duration_cast<std::chrono::milliseconds>(finish - start);
    cout << "- Duration = " << microseconds.count() << " ms" << endl;
    cout << "- Read Blocks = " << table.GetLastQueryBlockReadAccessCount() << endl;
    cout << "- Write Blocks = " << table.GetLastQueryBlockWriteAccessCount() << endl;
    cout << "- Deleted Records = " << records << endl;
    cout << endl;
}

void printRecords(vector<Record*> records, string label)
{
    cout << "- " << label << " (" << records.size() << ") = [ ";
    for (auto record : records)
    {
        cout << endl;
        record->Write(cout);
        cout << ",";
    }
    cout << '\b' << "]" << endl;
}
// DatabaseSystem.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include <iostream>
#include "pch.h"
#include "FixedRecord.h"
#include "../DatabaseSystem.Core/Table.h"
#include "../DatabaseSystems.Heap/HeapRecordManager.h"
#include "../DatabaseSystem.Ordered/OrderedRecordManager.h"
#include "VarRecord.h"
#include "../DatabaseSystems.HeapVar/HeapVarRecordManager.h"

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
void getAllBlockRecords(Table& table);

int main()
{
    auto varSchema = VarRecord::CreateSchema();

    auto dbPath = ".\\test.db";
    auto heap = HeapVarRecordManager(4096, 0);
    auto table = Table(heap);
    //table.Load(dbPath);
    table.Create(dbPath, varSchema);
    auto records = Record::LoadFromCsv(*varSchema, ".\\cbd.csv", -1);

    insertMany(table, records);
    
    //findOne(table);
    //findAllSet(table);
    //findAllBetween(table);
    findAllEquals(table);
    //deleteOne(table);
    deleteAllEquals(table);

   

    //findAllEquals(table);
    //insertMany(table, records);

    //table.Close();
    //return 0;
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


void getAllBlockRecords(Table& table)
{
    cout << "[Block] Get All Block Records" << endl;
    unsigned long long blockIdReq = 0;
    cout << "- BlockId = " << blockIdReq << endl;

    auto start = std::chrono::high_resolution_clock::now();
    auto records = table.SelectBlockRecords(blockIdReq);
    auto finish = std::chrono::high_resolution_clock::now();
    auto microseconds = std::chrono::duration_cast<std::chrono::milliseconds>(finish - start);
    cout << "- Duration = " << microseconds.count() << " ms" << endl;
    cout << "- Accessed Blocks = " << table.GetLastQueryBlockReadAccessCount() << endl;
    cout << "- Write Blocks = " << table.GetLastQueryBlockWriteAccessCount() << endl;
    printRecords(records);
    cout << endl;

}

void findOne(Table& table)
{
    cout << "[Find] Random Id" << endl;

    //std::random_device rd; // obtain a random number from hardware
    //std::mt19937 gen(rd()); // seed the generator
    //std::uniform_int_distribution<unsigned long long> distr(0, 100000); // define the range

    auto id = 42561;
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

    vector<unsigned long long> ids;

    ids.push_back(10);
    ids.push_back(223);
    ids.push_back(34567);
    ids.push_back(14);
    ids.push_back(202);
    ids.push_back(1112);

    auto start = std::chrono::high_resolution_clock::now();
    auto records = table.Select(ids);
    auto finish = std::chrono::high_resolution_clock::now();
    auto microseconds = std::chrono::duration_cast<std::chrono::milliseconds>(finish - start);
    cout << "- Duration = " << microseconds.count() << " ms" << endl;
    cout << "- Read Blocks = " << table.GetLastQueryBlockReadAccessCount() << endl;
    cout << "- Write Blocks = " << table.GetLastQueryBlockWriteAccessCount() << endl;
    //printRecords(records);
    cout << endl;
}

void findAllBetween(Table& table)
{
    unsigned long long min = 117;
    unsigned long long max = 200;
    cout << "[FindAll] Id Between " << min << " - " << max << endl;

    auto start = std::chrono::high_resolution_clock::now();
    auto records = table.SelectWhereBetween(NAMEOF(FixedRecord::Id).str(), SPANOF(min), SPANOF(max));
    auto finish = std::chrono::high_resolution_clock::now();
    auto microseconds = std::chrono::duration_cast<std::chrono::milliseconds>(finish - start);
    cout << "- Duration = " << microseconds.count() << " ms" << endl;
    cout << "- Read Blocks = " << table.GetLastQueryBlockReadAccessCount() << endl;
    cout << "- Write Blocks = " << table.GetLastQueryBlockWriteAccessCount() << endl;
    //printRecords(records);
    cout << endl;
}

void findAllEquals(Table& table)
{
    //char city[40] = "Sorocaba";
    //cout << "[FindAll] City = " << city << endl;
    
    float weight = 117;
    auto start = std::chrono::high_resolution_clock::now();
    auto records = table.SelectWhereEquals(NAMEOF(FixedRecord::Weigth).str(), SPANOF(weight));
    auto finish = std::chrono::high_resolution_clock::now();
    auto microseconds = std::chrono::duration_cast<std::chrono::milliseconds>(finish - start);
    cout << "- Duration = " << microseconds.count() << " ms" << endl;
    cout << "- Read Blocks = " << table.GetLastQueryBlockReadAccessCount() << endl;
    cout << "- Write Blocks = " << table.GetLastQueryBlockWriteAccessCount() << endl;
    //printRecords(records);
    cout << endl;
}

void deleteOne(Table& table)
{
    auto id = 53428;
    cout << "[DeleteOne] Id = " << id << endl;

    auto start = std::chrono::high_resolution_clock::now();
    table.Delete(id);
    auto finish = std::chrono::high_resolution_clock::now();
    auto microseconds = std::chrono::duration_cast<std::chrono::milliseconds>(finish - start);
    cout << "- Duration = " << microseconds.count() << " ms" << endl;
    cout << "- Read Blocks = " << table.GetLastQueryBlockReadAccessCount() << endl;
    cout << "- Write Blocks = " << table.GetLastQueryBlockWriteAccessCount() << endl;
    cout << endl;
}

void deleteAllEquals(Table& table)
{
    float weight = 117.0;
    cout << "[DeleteAll] Weight = " << weight << endl;
    

    auto start = std::chrono::high_resolution_clock::now();
    auto records = table.DeleteWhereEquals(NAMEOF(FixedRecord::Weigth).str(), SPANOF(weight));
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
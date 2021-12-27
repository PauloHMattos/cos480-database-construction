// DatabaseSystem.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include <iostream>
#include "pch.h"
#include "../DatabaseSystem.Core/Table.h"
#include "../DatabaseSystems.Heap/HeapRecordManager.h"

void runBenchmark(Table& table);

#pragma pack(1)
struct SimpleRecord {
    unsigned long long Id;
    int IntValue;
    double DoubleValue;
};

int main()
{
    auto fixedSchema = Schema();
    fixedSchema.AddColumn("IntValue", ColumnType::INT32);
    fixedSchema.AddColumn("DoubleValue", ColumnType::DOUBLE);

    auto dbPath = "C:\\Users\\Paulo Mattos\\source\\repos\\DatabaseSystem\\DatabaseSystem\\x64\\Debug\\test.db";

    auto heap = HeapRecordManager(50);
    auto table = Table(heap);
    table.Create(dbPath, fixedSchema);
    //table.Load(dbPath);

    auto record = Record(fixedSchema);
    auto simpleRecord = record.As<SimpleRecord>();
    simpleRecord->Id = 99;
    simpleRecord->DoubleValue = 99.00;

    for (int i = 0; i < 10; i++)
    {
        simpleRecord->IntValue = i;
        table.Insert(record);
    }

    runBenchmark(table);
}

void runBenchmark(Table& table)
{
    auto r = table.Select({ 1, 5 });

    double value = 99.00;
    auto data = span<unsigned char>((unsigned char*)&value, sizeof(double));
    auto result = table.SelectWhereEquals("DoubleValue", data);
    cout << result.size() << endl;
}
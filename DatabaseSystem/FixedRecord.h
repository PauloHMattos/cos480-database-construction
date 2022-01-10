#pragma once

#include "nameof.hpp"
#include "../DatabaseSystem.Core/Record.h"
#include "../DatabaseSystem.Core/Schema.h"

#pragma pack(1)
struct FixedRecord {
    unsigned long long Id;
    char Gender;
    char FirstName[50];
    char LastName[50];
    char StreetAddress[100];
    char City[40];
    char State[3];
    char ZipCode[10];
    char Email[100];
    char Username[20];
    char Password[20];
    char PhoneNumber[18];
    int Age;
    float Weigth;
    int Height;

    static Schema* CreateSchema();
};


#pragma once
#include "../DatabaseSystem.Core/Record.h"
#include "../DatabaseSystem.Core/Block.h"

#define MAX_RECORDS 9

class Bucket
{
public:
	Bucket();
	~Bucket();
	unsigned int hash;
	//unsigned int recordsCount;
	unsigned long long blockNumber;
};
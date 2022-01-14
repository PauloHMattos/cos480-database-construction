#pragma once
#include "../DatabaseSystem.Core/Record.h"
#include "../DatabaseSystem.Core/Block.h"

#define MAX_RECORDS 9

class Bucket
{
public:
	Bucket(unsigned int bucketNumber);
	~Bucket();
	unsigned int bucketNumber;
	unsigned int recordsCount;
	Block* block;
	unsigned int blockNumber;
	vector<unsigned long long> keys;
};
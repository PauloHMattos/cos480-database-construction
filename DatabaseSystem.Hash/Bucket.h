#pragma once
#include "../DatabaseSystem.Core/Record.h"
#include "../DatabaseSystem.Core/Block.h"


class Bucket
{
public:
	Bucket();
	~Bucket();
	unsigned int hash;
	unsigned long long blockNumber;
};
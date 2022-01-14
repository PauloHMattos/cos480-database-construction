#include "pch.h"
#include "Bucket.h"

Bucket::Bucket(unsigned int bucketNumber) : keys(MAX_RECORDS), recordsCount(0)
{
	this->bucketNumber = bucketNumber;
}
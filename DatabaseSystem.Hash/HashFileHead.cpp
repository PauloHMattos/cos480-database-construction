#include "pch.h"
#include "HashFileHead.h"

HashFileHead::HashFileHead(Schema* schema) : Buckets(vector<Bucket>())
{
	m_Schema = schema;
}

void HashFileHead::Serialize(iostream& dst)
{
	FileHead::Serialize(dst);
	dst << Buckets.size() << endl;
	for (auto bucket : Buckets) {
		dst << bucket.hash << endl;
		dst << bucket.blockNumber << endl;
	}
}

void HashFileHead::SetBucketCount(int count) {
	Buckets.reserve(count);
	for (int i = 0; i < count; i++) 
	{
		auto bucket = Bucket();
		bucket.hash = i;
		bucket.blockNumber = -1;
		Buckets.push_back(bucket);
	}
}

void HashFileHead::Deserialize(iostream& src)
{
	FileHead::Deserialize(src);
	int bucketCount;
	src >> bucketCount;
	Buckets.clear();
	Buckets.reserve(bucketCount);
	for (int i = 0; i < bucketCount; i++) {
		auto bucket = Bucket();
		src >> bucket.hash;
		src >> bucket.blockNumber;
		Buckets.push_back(bucket);
	}
}
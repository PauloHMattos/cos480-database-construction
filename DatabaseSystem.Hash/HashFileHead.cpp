#include "pch.h"
#include "HashFileHead.h"

HashFileHead::HashFileHead() : NextId(0), Buckets(vector<Bucket>())
{
}

HashFileHead::HashFileHead(Schema* schema) : NextId(0), Buckets(vector<Bucket>())
{
	m_Schema = schema;
}

HashFileHead::~HashFileHead()
{
}

void HashFileHead::Serialize(iostream& dst)
{
	FileHead::Serialize(dst);
	dst.write((const char*)&NextId, sizeof(NextId));

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
	src.read((char*)&NextId, sizeof(NextId));

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
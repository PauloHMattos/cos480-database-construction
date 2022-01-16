#include "pch.h"
#include "HashFileHead.h"

HashFileHead::HashFileHead() : NextId(0)
{
}

HashFileHead::HashFileHead(Schema* schema) : NextId(0)
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
}

void HashFileHead::Deserialize(iostream& src)
{
	FileHead::Deserialize(src);
	src.read((char*)&NextId, sizeof(NextId));

	int bucketCount;
	src >> bucketCount;
	Buckets.reserve(bucketCount);
	for (int i = 0; i < bucketCount; i++) {
		auto bucket = Bucket();
		src >> bucket.hash;
		src >> bucket.blockNumber;
		Buckets[i] = bucket;
	}
}
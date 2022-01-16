#pragma once
#include "../DatabaseSystem.Core/FileHead.h"
#include "Bucket.h"

class HashFileHead : public FileHead
{
public:
	HashFileHead();
	HashFileHead(Schema* schema);
	~HashFileHead();
	unsigned long long NextId;
	vector<Bucket> Buckets;

	void SetBucketCount(int count);
	// Inherited via FileHead
	virtual void Serialize(iostream& dst) override;
	virtual void Deserialize(iostream& src) override;
};
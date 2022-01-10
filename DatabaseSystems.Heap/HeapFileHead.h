#pragma once
#include "../DatabaseSystem.Core/FileHead.h"

class HeapFileHead : public FileHead
{
public:
	HeapFileHead();
	HeapFileHead(Schema* schema);
	~HeapFileHead();
	unsigned long long NextId;

	// Inherited via FileHead
	virtual void Serialize(iostream& dst) override;
	virtual void Deserialize(iostream& src) override;
};


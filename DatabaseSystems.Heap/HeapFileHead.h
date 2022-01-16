#pragma once
#include "../DatabaseSystem.Core/FileHead.h"


struct RecordPointer
{
	unsigned long long BlockId;
	unsigned long long RecordNumberInBlock;

	RecordPointer() 
	{
		BlockId = 0;
		RecordNumberInBlock = 0;
	}
};


class HeapFileHead : public FileHead
{
public:
	HeapFileHead(Schema* schema);
	~HeapFileHead();
	unsigned long long RemovedCount;
	RecordPointer RemovedRecordHead;
	RecordPointer RemovedRecordTail;

	// Inherited via FileHead
	virtual void Serialize(iostream& dst) override;
	virtual void Deserialize(iostream& src) override;
};


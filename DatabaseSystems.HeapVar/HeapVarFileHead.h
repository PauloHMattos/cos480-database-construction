#pragma once
#include "../DatabaseSystem.Core/FileHead.h"


struct RecordVarPointer
{
	unsigned long long BlockId;
	unsigned long long RecordNumberInBlock;

	RecordVarPointer()
	{
		BlockId = 0;
		RecordNumberInBlock = 0;
	}
};


class HeapVarFileHead : public FileHead
{
public:

	HeapVarFileHead();
	HeapVarFileHead(Schema* schema);
	~HeapVarFileHead();
	unsigned long long NextId;
	unsigned long long RemovedCount;
	RecordVarPointer RemovedRecordHead;
	RecordVarPointer RemovedRecordTail;

	// Inherited via FileHead
	virtual void Serialize(iostream& dst) override;
	virtual void Deserialize(iostream& src) override;
};


#include "pch.h"
#include "HeapVarFileHead.h"

HeapVarFileHead::HeapVarFileHead() : HeapVarFileHead(nullptr)
{
}

HeapVarFileHead::HeapVarFileHead(Schema* schema) : NextId(0), RemovedRecordHead(RecordVarPointer())
{
    m_Schema = schema;
}

HeapVarFileHead::~HeapVarFileHead()
{
}

void HeapVarFileHead::Serialize(iostream& dst)
{
    FileHead::Serialize(dst);
    dst << NextId << endl;
    dst << RemovedCount << endl;
    dst << RemovedRecordHead.BlockId << endl;
    dst << RemovedRecordHead.RecordNumberInBlock << endl;
    dst << RemovedRecordTail.BlockId << endl;
    dst << RemovedRecordTail.RecordNumberInBlock << endl;
}

void HeapVarFileHead::Deserialize(iostream& src)
{
    FileHead::Deserialize(src);
    src >> NextId;
    src >> RemovedCount;
    src >> RemovedRecordHead.BlockId;
    src >> RemovedRecordHead.RecordNumberInBlock;
    src >> RemovedRecordTail.BlockId;
    src >> RemovedRecordTail.RecordNumberInBlock;
}

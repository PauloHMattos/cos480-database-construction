#include "pch.h"
#include "HeapFileHead.h"

HeapFileHead::HeapFileHead(Schema* schema) : 
    NextId(0), 
    RemovedRecordHead(RecordPointer()), 
    RemovedCount(0)
{
    m_Schema = schema;
}

HeapFileHead::~HeapFileHead()
{
}

void HeapFileHead::Serialize(iostream& dst)
{
    FileHead::Serialize(dst);
    dst << NextId << endl;
    dst << RemovedCount << endl;
    dst << RemovedRecordHead.BlockId << endl;
    dst << RemovedRecordHead.RecordNumberInBlock << endl;
    dst << RemovedRecordTail.BlockId << endl;
    dst << RemovedRecordTail.RecordNumberInBlock << endl;
}

void HeapFileHead::Deserialize(iostream& src)
{
    FileHead::Deserialize(src);
    src >> NextId;
    src >> RemovedCount;
    src >> RemovedRecordHead.BlockId;
    src >> RemovedRecordHead.RecordNumberInBlock;
    src >> RemovedRecordTail.BlockId;
    src >> RemovedRecordTail.RecordNumberInBlock;
}

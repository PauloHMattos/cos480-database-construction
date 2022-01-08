#include "pch.h"
#include "HeapFileHead.h"

HeapFileHead::HeapFileHead() : NextId(0)
{
}

HeapFileHead::HeapFileHead(Schema schema)
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
}

void HeapFileHead::Deserialize(iostream& src)
{
    FileHead::Deserialize(src);
    src >> NextId;
}

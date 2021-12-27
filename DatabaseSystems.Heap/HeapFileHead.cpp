#include "pch.h"
#include "HeapFileHead.h"

HeapFileHead::HeapFileHead()
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
    dst.write((const char*)&NextId, sizeof(NextId));
}

void HeapFileHead::Deserialize(iostream& src)
{
    FileHead::Deserialize(src);
    src.read((char*)&NextId, sizeof(NextId));
}

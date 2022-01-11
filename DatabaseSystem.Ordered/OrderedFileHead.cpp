#include "pch.h"
#include "OrderedFileHead.h"

OrderedFileHead::OrderedFileHead()
{
}

OrderedFileHead::OrderedFileHead(Schema *schema) : NextId(0)
{
  m_Schema = schema;
}

OrderedFileHead::~OrderedFileHead()
{
}

void OrderedFileHead::Serialize(iostream &dst)
{
  FileHead::Serialize(dst);
  dst.write((const char *)&NextId, sizeof(NextId));
}

void OrderedFileHead::Deserialize(iostream &src)
{
  FileHead::Deserialize(src);
  src.read((char *)&NextId, sizeof(NextId));
}
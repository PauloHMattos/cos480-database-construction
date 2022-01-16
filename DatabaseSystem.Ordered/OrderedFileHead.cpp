#include "pch.h"
#include "OrderedFileHead.h"

OrderedFileHead::OrderedFileHead(Schema *schema)
{
  m_Schema = schema;
}

OrderedFileHead::~OrderedFileHead()
{
}

void OrderedFileHead::Serialize(iostream &dst)
{
  FileHead::Serialize(dst);
}

void OrderedFileHead::Deserialize(iostream &src)
{
  FileHead::Deserialize(src);
}
#include "pch.h"
#include "HashFileHead.h"

HashFileHead::HashFileHead() : NextId(0)
{
}

HashFileHead::HashFileHead(Schema* schema) : NextId(0)
{
	m_Schema = schema;
}

HashFileHead::~HashFileHead()
{
}

void HashFileHead::Serialize(iostream& dst)
{
	FileHead::Serialize(dst);
	dst.write((const char*)&NextId, sizeof(NextId));
}

void HashFileHead::Deserialize(iostream& src)
{
	FileHead::Deserialize(src);
	src.read((char*)&NextId, sizeof(NextId));
}
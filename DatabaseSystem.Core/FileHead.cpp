#include "pch.h"
#include "FileHead.h"

Schema& FileHead::GetSchema()
{
	return m_Schema;
}
//
//unsigned int FileHead::getRecordsCount()
//{
//	return m_RecordsCount;
//}
//
//void FileHead::setRecordsCount(unsigned int newCount)
//{
//	m_RecordsCount = newCount;
//}
//
//unsigned int FileHead::getRecordStartOffset()
//{
//	return m_RecordsStartOffset;
//}

void FileHead::Serialize(iostream& dst)
{
	m_Schema.Serialize(dst);
	dst << m_BlocksCount;
	//dst.write((const char*)&m_BlocksCount, sizeof(m_BlocksCount));
	//dst.write(reinterpret_cast<const char*>(&m_RecordsStartOffset), sizeof(m_RecordsStartOffset));
}

void FileHead::Deserialize(iostream& src)
{
	m_Schema.Deserialize(src);
	src >> m_BlocksCount;
	//src.read(reinterpret_cast<char*>(&m_RecordsStartOffset), sizeof(m_RecordsStartOffset));
}

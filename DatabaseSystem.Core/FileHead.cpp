#include "pch.h"
#include "FileHead.h"

Schema* FileHead::GetSchema()
{
	return m_Schema;
}

void FileHead::Serialize(iostream& dst)
{
	m_Schema->Serialize(dst);
	dst << m_BlocksCount << endl;
}

void FileHead::Deserialize(iostream& src)
{
	m_Schema->Deserialize(src);
	src >> m_BlocksCount;
}

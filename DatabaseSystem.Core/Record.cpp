#include "pch.h"
#include "Record.h"


Record::Record(Schema& schema) : m_Schema(schema)
{
	m_Data = vector<unsigned char>(m_Schema.GetSize());
}

vector<unsigned char>* Record::GetData()
{
	return &m_Data;
}

void Record::SetData(size_t index, unsigned char value)
{
	m_Data[index] = value;
}

unsigned long long Record::getId() const
{
	return *(unsigned long long*)(m_Data.data());
}
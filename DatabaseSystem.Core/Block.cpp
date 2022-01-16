#include "pch.h"
#include "Block.h"

Block::Block(unsigned int blockSize, unsigned int recordSize, unsigned int headerSize) : m_RecordSize(recordSize), m_HeaderSize(headerSize)
{
	m_CurrentLengthInBytes = sizeof(unsigned int) + sizeof(headerSize);
	m_BlockData.resize(blockSize);
}

void Block::Load(span<unsigned char> data)
{
	Clear();
	memcpy(&m_BlockData[0], &data[0], data.size());

	auto recordsCount = ((unsigned int*)data.data())[0];
	data = span(m_BlockData).subspan(sizeof(recordsCount));

	for (int i = 0; i < recordsCount; i++)
	{
		auto recordData = new span<unsigned char>(data.subspan(i * m_RecordSize, m_RecordSize));
		m_Records.Insert(recordData);
		m_CurrentLengthInBytes += m_RecordSize;
		m_Records.Advance();
	}
}

void Block::Flush(span<unsigned char> data)
{
	auto recordsCount = GetRecordsCount();
	((unsigned int*)m_BlockData.data())[0] = recordsCount;
	memcpy(data.data(), m_BlockData.data(), m_BlockData.size());
}

void Block::Clear()
{
	m_Records.MoveToStart();
	while (m_Records.RightLength() > 0)
	{
		m_Records.Remove();
	}
	m_CurrentLengthInBytes = sizeof(unsigned int) + sizeof(m_HeaderSize);
	memset(m_BlockData.data(), 0, m_BlockData.size());
}

void Block::Append(span<unsigned char> data)
{
	memcpy(&m_BlockData[m_CurrentLengthInBytes], &data[0], data.size());

	auto recordData = new span<unsigned char>(&m_BlockData[m_CurrentLengthInBytes], m_RecordSize);

	m_Records.MoveToFinish();
	m_Records.Insert(recordData);
	m_CurrentLengthInBytes += m_RecordSize;
}

void Block::MoveToStart()
{
	m_Records.MoveToStart();
}

int Block::GetRecordsCount()
{
	return m_Records.LeftLength() + m_Records.RightLength();
}

bool Block::GetRecord(vector<unsigned char>* record)
{
	if (m_Records.RightLength()) {
		memcpy(record->data(), m_Records.Current(0)->data(), record->size());
		m_Records.Advance();
		return true;
	}
	return false;
}

span<unsigned char> Block::GetHeader() {
	return span(m_BlockData).subspan(sizeof(unsigned int), m_HeaderSize);
}
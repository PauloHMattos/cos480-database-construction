#include "pch.h"
#include "Block.h"

Block::Block(unsigned int blockSize, unsigned int recordSize, unsigned int headerSize) : m_RecordSize(recordSize), m_HeaderSize(headerSize)
{
	m_CurrentLengthInBytes = sizeof(unsigned int) + headerSize;
	m_BlockData.resize(blockSize);
}

void Block::Load(span<unsigned char> data)
{
	Clear();
	memcpy(&m_BlockData[0], &data[0], data.size());

	auto recordsCount = ((unsigned int*)data.data())[0];
	data = span(m_BlockData).subspan(sizeof(recordsCount) + m_HeaderSize);

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
	m_CurrentLengthInBytes = sizeof(unsigned int) + m_HeaderSize;
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

void Block::MoveToEnd()
{
	m_Records.MoveToFinish();
}

void Block::Retreat()
{
	m_Records.Retreat();
}

void Block::Advance()
{
	m_Records.Advance();
}

void Block::Remove()
{
	m_Records.Remove();
}

unsigned int Block::GetRecordsCount()
{
	return (unsigned int)(m_Records.LeftLength() + m_Records.RightLength());
}

bool Block::GetRecord(vector<unsigned char>* record)
{
	if (m_Records.RightLength() > 0) {
		memcpy(record->data(), m_Records.Current(0)->data(), record->size());
		m_Records.Advance();
		return true;
	}
	return false;
}

int Block::GetPosition()
{
	return m_Records.LeftLength();
}

bool Block::GetRecordSpan(unsigned long long recordNumberInBlock, span<unsigned char>* record)
{
	if (GetRecordsCount() <= recordNumberInBlock)
	{
		return false;
	}

	m_Records.MoveToStart();
	//auto currentPosition = m_Records.LeftLength();
	//auto peekPosition = recordNumberInBlock - currentPosition;
	*record = *m_Records.Current(recordNumberInBlock);
	return true;
}

bool Block::GetRecordBack(vector<unsigned char>* record)
{
	if (m_Records.LeftLength() >= 0 && m_Records.LeftLength() < GetRecordsCount())
	{
		memcpy(record->data(), m_Records.Current(0)->data(), record->size());
		m_Records.Retreat();
		return true;
	}
	return false;
}

bool Block::GetCurrentSpan(span<unsigned char>* record)
{
	if (GetRecordsCount() == 0)
	{
		return false;
	}

	*record = *m_Records.Current(0);
	return true;
}

bool Block::RemoveRecordAt(unsigned long long recordNumber)
{
	if (GetRecordsCount() <= recordNumber)
	{
		return false;
	}

	m_Records.MoveToStart();
	//auto currentPosition = m_Records.LeftLength();
	//auto peekPosition = recordNumber - currentPosition;
	auto record = m_Records.Current(recordNumber);

	if (recordNumber == GetRecordsCount() - 1)
	{
		// Last record in block
		// Clear (maybe not needed)
		memset(record->data(), 0, record->size());
		m_Records.MoveToFinish();
		m_Records.Remove();
	}
	else
	{
		// Not the last one
		// Swap the last with the one to remove
		m_Records.MoveToFinish();
		auto last = m_Records.Current(0);
		memcpy(record->data(), last->data(), record->size());
		m_Records.Remove();
	}
	return true;
}

bool Block::MoveToAndGetRecord(unsigned int recordNumberInBlock, vector<unsigned char>* record)
{
	MoveToStart();
	auto size = m_Records.LeftLength() + m_Records.RightLength();
	if (recordNumberInBlock <= GetRecordsCount())
	{
		for (int i = 0; i < recordNumberInBlock; i++) {
			m_Records.Advance();
		}
		memcpy(record->data(), m_Records.Current(0)->data(), record->size());
		return true;
	}
	return false;
}

span<unsigned char> Block::GetHeader() {
	return span(m_BlockData).subspan(sizeof(unsigned int), m_HeaderSize);
}
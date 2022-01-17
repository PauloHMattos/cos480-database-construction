#include "pch.h"
#include "Block.h"

Block::Block(unsigned int blockSize, unsigned int recordSize) : m_RecordSize(recordSize)
{
	// RecordsCount, FinishRecordMap and StartRecords
	m_MetaDataCount = 3;
	m_CurrentLengthInBytes = m_MetaDataCount * sizeof(unsigned int);
	m_FinishRecordMap = m_CurrentLengthInBytes;
	m_StartRecords = blockSize - 1;
	m_BlockData.resize(blockSize);
}

void Block::Load(span<unsigned char> data)
{
	Clear();
	memcpy(&m_BlockData[0], &data[0], data.size());

	auto recordsCount = ((unsigned int*)data.data())[0];
	auto finishMap = ((unsigned int*)data.data())[1];
	auto startRecords = ((unsigned int*)data.data())[2];
	
	auto headSize = m_MetaDataCount * sizeof(unsigned int);
	m_RecordsCount = recordsCount;
	m_FinishRecordMap = finishMap;
	m_StartRecords = startRecords;

	data = span(m_BlockData);

	auto recordsMap = data.subspan(m_CurrentLengthInBytes, finishMap - m_CurrentLengthInBytes);
	for (int i = 0; i < recordsMap.size(); i += sizeof(struct BlockRecordMap))
	{
		auto blockRecordMap = (BlockRecordMap*)recordsMap.subspan(i, sizeof(struct BlockRecordMap)).data();
		auto recordData = new span<unsigned char>(data.subspan(blockRecordMap->RecordStart, blockRecordMap->RecordLength));
		m_Records.Insert(recordData);
		m_CurrentLengthInBytes += blockRecordMap->RecordLength;
		m_Records.Advance();
	}
}

void Block::Flush(span<unsigned char> data)
{
	auto recordsCount = GetRecordsCount();
	((unsigned int*)m_BlockData.data())[0] = recordsCount;
	((unsigned int*)m_BlockData.data())[1] = m_FinishRecordMap;
	((unsigned int*)m_BlockData.data())[2] = m_StartRecords;
	memcpy(data.data(), m_BlockData.data(), m_BlockData.size());
}

void Block::Clear()
{
	m_Records.MoveToStart();
	while (m_Records.RightLength() > 0)
	{
		m_Records.Remove();
	}
	m_CurrentLengthInBytes = m_MetaDataCount * sizeof(unsigned int);
	m_FinishRecordMap = m_CurrentLengthInBytes;
	m_StartRecords = m_BlockData.size() - 1;
	memset(m_BlockData.data(), 0, m_BlockData.size());
}

void Block::Append(span<unsigned char> data)
{
	auto newStartRecords = m_StartRecords - data.size();
	memcpy(&m_BlockData[newStartRecords], &data[0], data.size());

	auto recordData = new span<unsigned char>(&m_BlockData[m_CurrentLengthInBytes], data.size());

	// Register record
	BlockRecordMap recordMap;
	recordMap.RecordStart = m_StartRecords - data.size();
	recordMap.RecordLength = data.size();
	memcpy(&m_BlockData[m_FinishRecordMap], &recordMap, sizeof(struct BlockRecordMap));

	m_FinishRecordMap += sizeof(struct BlockRecordMap);
	m_StartRecords = newStartRecords;

	m_Records.MoveToFinish();
	m_Records.Insert(recordData);
	m_CurrentLengthInBytes += data.size();
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

void Block::Remove()
{
	m_Records.Remove();
}

unsigned int Block::GetRecordsCount()
{
	return (unsigned int)(m_Records.LeftLength() + m_Records.RightLength());
}

unsigned int Block::GetCurrRecordSize()
{
	if (m_Records.RightLength() > 0) {
		return m_Records.Current(0)->size();
	}
	return 0;
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
	if (m_Records.LeftLength())
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

size_t Block::GetCurrentLengthInBytes()
{
	return m_CurrentLengthInBytes;
}

unsigned int Block::GetBlockSize() const
{
	return m_BlockData.size();
}

unsigned int Block::GetFinishRecordMap() const
{
	return m_FinishRecordMap;
}

unsigned int Block::GetStartRecords() const
{
	return m_StartRecords;
}

void Block::UpdateRecordMapPos(unsigned int recordNumber, unsigned int bytesShifted)
{
	auto offSetToUpdate = m_MetaDataCount + (2 * recordNumber);
	((unsigned int*)m_BlockData.data())[offSetToUpdate] += bytesShifted;
}

void Block::ShiftBytes(vector<unsigned char> &buffer, unsigned int currPos, unsigned int length, unsigned int bytesShifted, bool isLeftDir)
{
	memcpy(buffer.data(), &m_BlockData.data()[currPos], length);
	memcpy(&m_BlockData.data()[currPos + ((isLeftDir ? -1 : 1) * bytesShifted)], &buffer.data()[0], length);
}

void Block::RemoveRecordMap(vector<unsigned char>& buffer, unsigned int recordNumber)
{
	auto currPos = (m_MetaDataCount * sizeof(unsigned int)) + ((recordNumber + 1) * sizeof(struct BlockRecordMap));
	auto length = m_FinishRecordMap - currPos;
	auto bytesShifted = sizeof(struct BlockRecordMap);
	ShiftBytes(buffer, currPos, length, bytesShifted, 1);
	m_FinishRecordMap -= sizeof(struct BlockRecordMap);
}

unsigned int Block::GetDataRecordsCount()
{
	return (unsigned int)m_BlockData[0];
}

long long Block::GetVarRecordId(unsigned int recordNumber)
{
	auto currPos = ((unsigned int*)m_BlockData.data())[m_MetaDataCount + (2 * (recordNumber - 1))];
	auto id = ((long long *)span(&m_BlockData.data()[currPos], sizeof(unsigned long long)).data())[0];
	return id;
}

unsigned int Block::GetRecordPos(unsigned int recordNumber)
{
	return ((unsigned int*)m_BlockData.data())[m_MetaDataCount + (2 * (recordNumber - 1))];
}

unsigned int Block::GetRecordLength(unsigned int recordNumber)
{
	return ((unsigned int*)m_BlockData.data())[m_MetaDataCount + (2 * (recordNumber - 1)) + 1];
}

void Block::UpdateStartRecordPos(unsigned int newPos)
{
	m_StartRecords = newPos;
}

void Block::PrintRecordMapper()
{
	auto data = span(m_BlockData);
	auto recordsMap = data.subspan(12, m_RecordsCount * sizeof(struct BlockRecordMap));
	for (int i = 0; i < recordsMap.size(); i += sizeof(struct BlockRecordMap))
	{
		auto blockRecordMap = (BlockRecordMap*)recordsMap.subspan(i, sizeof(struct BlockRecordMap)).data();
		printf("Block id: %d. Start: %d. Length: %d.\n", i / sizeof(struct BlockRecordMap), blockRecordMap->RecordStart, blockRecordMap->RecordLength);
	}
}

void Block::UpdateRecordsCount(unsigned int newCount)
{
	m_RecordsCount = newCount;
}

unsigned int Block::GetRecordsCountVar()
{
	return m_RecordsCount;
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

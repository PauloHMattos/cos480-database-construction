
#include "pch.h"
#include "BaseRecordManager.h"

BaseRecordManager::BaseRecordManager() :
	m_ReadBlock(nullptr),
	m_WriteBlock(nullptr),
	m_RecordsPerBlock(0),
	m_LastQueryBlockAccessCount(0),
	m_NextReadBlockNumber(0)
{
}

void BaseRecordManager::Create(string path, Schema* schema)
{
	GetFile()->NewFile(path, CreateNewFileHead(schema));

	auto schemaSize = GetSchema()->GetSize();
	auto blockLength = GetFile()->GetBlockSize();
	auto blockContentLength = GetFile()->GetBlockSize() - sizeof(unsigned int);
	m_RecordsPerBlock = floor(blockContentLength / schemaSize);

	m_ReadBlock = GetFile()->CreateBlock();
	m_WriteBlock = GetFile()->CreateBlock();
}

void BaseRecordManager::Open(string path)
{
	GetFile()->Open(path, CreateNewFileHead(nullptr));

	auto schemaSize = GetSchema()->GetSize();
	auto blockLength = GetFile()->GetBlockSize();
	auto blockContentLength = GetFile()->GetBlockSize() - sizeof(unsigned int);
	m_RecordsPerBlock = floor(blockContentLength / schemaSize);

	m_ReadBlock = GetFile()->CreateBlock();
	m_WriteBlock = GetFile()->CreateBlock();
}

void BaseRecordManager::Close()
{
	auto recordsCount = m_WriteBlock->GetRecordsCount();
	if (recordsCount > 0)
	{
		GetFile()->AddBlock(m_WriteBlock);
	}
	GetFile()->Close();
}

Schema* BaseRecordManager::GetSchema()
{
	return GetFile()->GetHead()->GetSchema();
}

unsigned long long BaseRecordManager::GetSize()
{
	auto writtenBlocks = GetFile()->GetBlockSize() * GetFile()->GetHead()->GetBlocksCount();
	auto recordsToBeWritten = m_WriteBlock->GetRecordsCount() * GetSchema()->GetSize();
	return writtenBlocks + recordsToBeWritten;
}


unsigned long long BaseRecordManager::GetLastQueryBlockAccessCount() const
{
	return m_LastQueryBlockAccessCount;
}

bool BaseRecordManager::MoveNext(Record* record, unsigned long long& accessedBlocks)
{
	unsigned long long blockId;
	unsigned long long recordNumberInBlock;
	return MoveNext(record, accessedBlocks, blockId, recordNumberInBlock);
}

void BaseRecordManager::InsertMany(vector<Record> records)
{
	m_LastQueryBlockAccessCount = 0;

	for (auto &record : records) 
	{
		Insert(record);
	}
}

Record* BaseRecordManager::Select(unsigned long long id)
{
	m_LastQueryBlockAccessCount = 0;
	unsigned long long accessedBlocks = 0;

	auto schema = GetSchema();
	auto currentRecord = new Record(schema);

	MoveToStart();
	while (MoveNext(currentRecord, accessedBlocks))
	{
		m_LastQueryBlockAccessCount += accessedBlocks;
		if (currentRecord->getId() == id) {
			return currentRecord;
		}
	}

	return nullptr;
}

vector<Record*> BaseRecordManager::Select(vector<unsigned long long> ids)
{
	unsigned long long accessedBlocks = 0;

	auto records = vector<Record*>();
	for (auto id : ids)
	{
		auto found = Select(id);
		accessedBlocks += m_LastQueryBlockAccessCount;

		if (found == nullptr) continue;
		records.push_back(found);
	}
	m_LastQueryBlockAccessCount = accessedBlocks;
	return records;
}

vector<Record*> BaseRecordManager::SelectWhereBetween(unsigned int columnId, span<unsigned char> min, span<unsigned char> max)
{
	m_LastQueryBlockAccessCount = 0;
	unsigned long long accessedBlocks = 0;

	auto records = vector<Record*>();
	auto schema = GetSchema();
	auto column = schema->GetColumn(columnId);

	auto currentRecord = Record(schema);

	MoveToStart();
	while (MoveNext(&currentRecord, accessedBlocks))
	{
		m_LastQueryBlockAccessCount += accessedBlocks;
		auto value = schema->GetValue(currentRecord.GetData(), columnId);

		if (Column::Compare(column, value, min) >= 0 && Column::Compare(column, value, max) <= 0)
		{
			auto newRecord = new Record(schema);
			memcpy(newRecord->GetData()->data(), currentRecord.GetData()->data(), schema->GetSize());
			records.push_back(newRecord);
		}
	}
	return records;
}

vector<Record*> BaseRecordManager::SelectWhereEquals(unsigned int columnId, span<unsigned char> data)
{
	m_LastQueryBlockAccessCount = 0;
	unsigned long long accessedBlocks = 0;

	auto records = vector<Record*>();
	auto schema = GetSchema();
	auto column = schema->GetColumn(columnId);

	auto currentRecord = Record(schema);

	MoveToStart();
	while (MoveNext(&currentRecord, accessedBlocks))
	{
		m_LastQueryBlockAccessCount += accessedBlocks;
		auto value = schema->GetValue(currentRecord.GetData(), columnId);

		if (Column::Equals(column, value, data))
		{
			auto newRecord = new Record(schema);
			memcpy(newRecord->GetData()->data(), currentRecord.GetData()->data(), schema->GetSize());
			records.push_back(newRecord);
		}
	}
	return records;
}

void BaseRecordManager::Delete(unsigned long long recordId)
{
	int removedCount = 0;
	m_LastQueryBlockAccessCount = 0;
	unsigned long long accessedBlocks = 0;

	auto schema = GetSchema();
	auto currentRecord = Record(schema);

	unsigned long long blockId;
	unsigned long long recordNumberInBlock;

	MoveToStart();
	while (MoveNext(&currentRecord, accessedBlocks, blockId, recordNumberInBlock))
	{
		m_LastQueryBlockAccessCount += accessedBlocks;
		if (currentRecord.getId() == recordId)
		{
			DeleteInternal(blockId, recordNumberInBlock);
			return;
		}
	}
}

int BaseRecordManager::DeleteWhereEquals(unsigned int columnId, span<unsigned char> data)
{
	int removedCount = 0;
	m_LastQueryBlockAccessCount = 0;
	unsigned long long accessedBlocks = 0;

	auto schema = GetSchema();
	auto column = schema->GetColumn(columnId);
	auto currentRecord = Record(schema);

	unsigned long long blockId;
	unsigned long long recordNumberInBlock;

	MoveToStart();
	while (MoveNext(&currentRecord, accessedBlocks, blockId, recordNumberInBlock))
	{
		m_LastQueryBlockAccessCount += accessedBlocks;

		auto value = schema->GetValue(currentRecord.GetData(), columnId);

		if (Column::Equals(column, value, data))
		{
			removedCount++;
			DeleteInternal(blockId, recordNumberInBlock);
		}
	}

	return removedCount;
}

void BaseRecordManager::MoveToStart()
{
	m_NextReadBlockNumber = 0;
}

bool BaseRecordManager::MoveNext(Record* record, unsigned long long& accessedBlocks, unsigned long long& blockId, unsigned long long& recordNumberInBlock)
{
	auto blocksCount = GetBlocksCount();
	auto initialBlock = m_NextReadBlockNumber;
	if (m_NextReadBlockNumber == 0)
	{
		//did not start to read before
		if (blocksCount > 0)
		{
			ReadNextBlock();
		}
	}

	bool returnVal = TryGetNextValidRecord(record);

	if (returnVal)
	{
		recordNumberInBlock = m_ReadBlock->GetPosition() - 1;
		blockId = m_NextReadBlockNumber - 1;
	}
	accessedBlocks = m_NextReadBlockNumber - initialBlock;
	return returnVal;
}

unsigned long long BaseRecordManager::GetBlocksCount()
{
	return GetFile()->GetHead()->GetBlocksCount();
}

void BaseRecordManager::ReadNextBlock()
{
	ReadBlock(m_NextReadBlockNumber);
	m_NextReadBlockNumber++;
}

void BaseRecordManager::ReadBlock(unsigned long long blockId)
{
	m_ReadBlock->Clear();
	GetFile()->GetBlock(blockId, m_ReadBlock);
	m_ReadBlock->MoveToStart();
	m_WriteBlock->MoveToStart();
}

bool BaseRecordManager::TryGetNextValidRecord(Record* record)
{
	auto recordData = record->GetData();

	// Search first get records from the write block
	// In case of a select, we might get lucky and the record
	// Was recently created and still is in memory
	// Searching in memory it's a lot faster then going
	// through the file
	if (m_WriteBlock->GetRecordsCount() > 0)
	{
		while (m_WriteBlock->GetPosition() < m_WriteBlock->GetRecordsCount())
		{
			if (m_WriteBlock->GetRecord(recordData))
			{
				// Here we dont have to check for the id == -1
				// When we remove records from the write block we dont set the id.
				// Just remove from the list
				return true;
			}
		}
	}

	auto blocksInFile = GetBlocksCount();
	while (blocksInFile > 0 && m_NextReadBlockNumber < blocksInFile - 1)
	{
		while (m_ReadBlock->GetRecord(recordData))
		{
			auto recordHeapData = record->As<BaseRecord>();
			if (recordHeapData->Id != -1)
			{
				return true;
			}
		}
		ReadNextBlock();
	}
	return false;
}
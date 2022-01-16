
#include "pch.h"
#include "BaseRecordManager.h"

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
	currentRecord->ResizeData(424);

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
			newRecord->ResizeData(424);
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

	//
	currentRecord.ResizeData(424);
	//
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

vector<Record*> BaseRecordManager::SelectBlock(unsigned long long blockIdReq)
{
	m_LastQueryBlockAccessCount = 0;
	unsigned long long accessedBlocks = 0;

	auto schema = GetSchema();
	auto currentRecord = Record(schema);

	//
	currentRecord.ResizeData(424);
	//

	auto records = vector<Record*>();
	unsigned long long blockId;
	unsigned long long recordNumberInBlock;

	MoveToStart();
	while (MoveNext(&currentRecord, accessedBlocks, blockId, recordNumberInBlock))
	{
		if (blockId == blockIdReq) {
			auto newRecord = new Record(schema);
			newRecord->ResizeData(424);
			memcpy(newRecord->GetData()->data(), currentRecord.GetData()->data(), 424);
			records.push_back(newRecord);
		}

		if (blockId > blockIdReq)
			break;
	}

	return records;
}
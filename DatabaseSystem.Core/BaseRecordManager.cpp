
#include "pch.h"
#include "BaseRecordManager.h"

unsigned long long BaseRecordManager::GetLastQueryBlockAccessCount() const
{
	return m_LastQueryBlockAccessCount;
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

int BaseRecordManager::DeleteWhereEquals(unsigned int columnId, span<unsigned char> data)
{
	m_LastQueryBlockAccessCount = 0;
	unsigned long long accessedBlocks = 0;

	auto records = vector<unsigned long long>();
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
			records.push_back(currentRecord.getId());
		}
	}

	for (auto id : records) {
		Delete(id);
	}

	return records.size();
}
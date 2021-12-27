
#include "pch.h"
#include "BaseRecordManager.h"

void BaseRecordManager::InsertMany(vector<Record> records)
{
	for (auto &record : records) 
	{
		Insert(record);
	}
}

Record* BaseRecordManager::Select(unsigned long long id)
{
	auto schema = GetSchema();
	auto currentRecord = new Record(schema);

	MoveToStart();
	while (MoveNext(currentRecord))
	{
		if (currentRecord->getId() == id) {
			return currentRecord;
		}
	}

	return nullptr;
}

vector<Record*> BaseRecordManager::Select(vector<unsigned long long> ids)
{
	auto records = vector<Record*>();
	for (auto id : ids)
	{
		auto found = Select(id);
		if (found == nullptr) continue;
		records.push_back(found);
	}
	return records;
}

vector<Record*> BaseRecordManager::SelectWhereBetween(unsigned int columnId, span<unsigned char> min, span<unsigned char> max)
{
	auto records = vector<Record*>();
	auto schema = GetSchema();
	auto column = schema.GetColumn(columnId);

	auto currentRecord = Record(schema);

	MoveToStart();
	while (MoveNext(&currentRecord))
	{
		auto value = schema.GetValue(currentRecord.GetData(), columnId);

		if (Column::Compare(column, value, min) >= 0 && Column::Compare(column, value, max) <= 0)
		{
			auto newRecord = new Record(schema);
			memcpy(newRecord->GetData()->data(), currentRecord.GetData()->data(), schema.GetSize());
			records.push_back(newRecord);
		}
	}


	return records;
}

vector<Record*> BaseRecordManager::SelectWhereEquals(unsigned int columnId, span<unsigned char> data)
{
	auto records = vector<Record*>();
	auto schema = GetSchema();
	auto column = schema.GetColumn(columnId);

	auto currentRecord = Record(schema);

	MoveToStart();
	while (MoveNext(&currentRecord))
	{
		auto value = schema.GetValue(currentRecord.GetData(), columnId);

		if (Column::Equals(column, value, data))
		{
			auto newRecord = new Record(schema);
			memcpy(newRecord->GetData()->data(), currentRecord.GetData()->data(), schema.GetSize());
			records.push_back(newRecord);
		}
	}
	return records;
}

int BaseRecordManager::DeleteWhereEquals(unsigned int columnId, span<unsigned char> data)
{
	auto records = vector<unsigned long long>();
	auto schema = GetSchema();
	auto column = schema.GetColumn(columnId);
	auto currentRecord = Record(schema);

	MoveToStart();
	while (MoveNext(&currentRecord))
	{
		auto value = schema.GetValue(currentRecord.GetData(), columnId);

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
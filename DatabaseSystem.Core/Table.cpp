#include "pch.h"
#include "Table.h"

Table::Table(BaseRecordManager& recordManager) : m_RecordManager(recordManager)
{
}

void Table::Load(string path)
{
    m_RecordManager.Open(path);
}

void Table::Close()
{
    m_RecordManager.Close();
}


unsigned long long Table::GetSize()
{
    return m_RecordManager.GetSize();
}

Schema* Table::GetSchema()
{
    return m_RecordManager.GetSchema();
}

unsigned long long Table::GetLastQueryBlockReadAccessCount()
{
    return m_RecordManager.GetLastQueryBlockReadAccessCount();
}

unsigned long long Table::GetLastQueryBlockWriteAccessCount()
{
    return m_RecordManager.GetLastQueryBlockWriteAccessCount();
}

void Table::Create(string path, Schema* schema)
{
    m_RecordManager.Create(path, schema);
}

void Table::Insert(Record record)
{
    m_RecordManager.Insert(record);
}

void Table::InsertMany(vector<Record> records)
{
    m_RecordManager.InsertMany(records);
}

Record* Table::Select(unsigned long long id)
{
    return m_RecordManager.Select(id);
}

vector<Record*> Table::Select(vector<unsigned long long> ids)
{
    return m_RecordManager.Select(ids);
}

vector<Record*> Table::SelectWhereBetween(string columnName, span<unsigned char> min, span<unsigned char> max)
{
    auto columnId = m_RecordManager.GetSchema()->GetColumnId(columnName);
    return m_RecordManager.SelectWhereBetween(columnId, min, max);
}

vector<Record*> Table::SelectWhereEquals(string columnName, span<unsigned char> data)
{
    auto columnId = m_RecordManager.GetSchema()->GetColumnId(columnName);
    return m_RecordManager.SelectWhereEquals(columnId, data);
}

void Table::Delete(unsigned long long id)
{
    m_RecordManager.Delete(id);
}

int Table::DeleteWhereEquals(string columnName, span<unsigned char> data)
{
    auto columnId = m_RecordManager.GetSchema()->GetColumnId(columnName);
    return m_RecordManager.DeleteWhereEquals(columnId, data);
}

vector<Record*> Table::SelectBlockRecords(unsigned long long blockId)
{
    return m_RecordManager.SelectBlock(blockId);
}
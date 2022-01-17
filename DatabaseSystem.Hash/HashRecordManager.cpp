#include "pch.h"
#include "HashRecordManager.h"
#include "../DatabaseSystem.Core/Assertions.h"
#include "HashFileHead.h"

HashRecordManager::HashRecordManager(size_t blockSize, unsigned int numberOfBuckets) :
    BaseRecordManager(),
    m_File(new FileWrapper<HashFileHead>(blockSize, sizeof(unsigned long long))),
    m_NumberOfBuckets(numberOfBuckets)
{
}

void HashRecordManager::Create(string path, Schema* schema)
{
    BaseRecordManager::Create(path, schema);
    m_File->GetHead()->SetBucketCount(m_NumberOfBuckets);
}

void HashRecordManager::Open(string path)
{
    BaseRecordManager::Open(path);
    Assert(m_NumberOfBuckets == m_File->GetHead()->Buckets.size(), "Buckets count missmatch");
}

unsigned int HashRecordManager::hashFunction(unsigned long long key)
{
    return key % m_NumberOfBuckets;
}

Record* HashRecordManager::Select(unsigned long long id)
{
    ClearAccessCount();
    unsigned int bucketNumber = hashFunction(id);
    auto record = new Record(GetSchema());
    m_NextReadBlockNumber = m_File->GetHead()->Buckets[bucketNumber].blockNumber;
    while (m_NextReadBlockNumber != -1) {
        if (!ReadBlock(m_ReadBlock, m_NextReadBlockNumber)) {
            Assert(false, "Invalid block");
            return nullptr;
        }
        m_ReadBlock->MoveToStart();
        while (m_ReadBlock->GetRecord(record->GetData())) {
            if (record->getId() == id) {
                return record;
            }
        }
        m_NextReadBlockNumber = *(unsigned long long*)m_ReadBlock->GetHeader().data();
    }
    return nullptr;
}

bool HashRecordManager::SelectSpan(unsigned long long id, span<unsigned char>* data)
{
    ClearAccessCount();
    unsigned int bucketNumber = hashFunction(id);
    auto record = new Record(GetSchema());
    auto blockNumber = m_File->GetHead()->Buckets[bucketNumber].blockNumber;
    while (blockNumber != -1) {
        if (!ReadBlock(m_ReadBlock, blockNumber)) {
            Assert(false, "Invalid block");
            return false;
        }
        m_ReadBlock->MoveToStart();
        while (m_ReadBlock->GetPosition() < m_ReadBlock->GetRecordsCount() && m_ReadBlock->GetCurrentSpan(data)) {
            if (record->getId() == id) {
                return true;
            }
            m_ReadBlock->Advance();
        }
        blockNumber = *(unsigned long long*)m_ReadBlock->GetHeader().data();
    }
    return false;
}

vector<Record*> HashRecordManager::SelectWhereEquals(unsigned int columnId, span<unsigned char> data)
{
    if (columnId != 0) {
        return BaseRecordManager::SelectWhereEquals(columnId, data);
    }
    ClearAccessCount();
    auto records = vector<Record*>();
    auto id = *(unsigned long long*)data.data();
    unsigned int bucketNumber = hashFunction(id);
    auto record = new Record(GetSchema());
    auto nextBucketBlockNumber = m_File->GetHead()->Buckets[bucketNumber].blockNumber;
    while (nextBucketBlockNumber != -1) {
        if (!ReadBlock(m_ReadBlock, nextBucketBlockNumber)) {
            Assert(false, "Invalid block");
            break;
        }
        m_ReadBlock->MoveToStart();
        while (m_ReadBlock->GetRecord(record->GetData())) {
            records.push_back(record);
        }
        nextBucketBlockNumber = *(unsigned long long*)m_ReadBlock->GetHeader().data();
    }
    return records;
}

void HashRecordManager::Insert(Record record)
{
    ClearAccessCount();

    auto hashRecord = record.As<HashRecord>();
    hashRecord->Id = m_File->GetHead()->NextId;
    m_File->GetHead()->NextId += 1;

    unsigned int bucketHash = hashFunction(hashRecord->Id);
    auto nextBucketBlockNumber = m_File->GetHead()->Buckets[bucketHash].blockNumber;
    unsigned long long previousBucketBlockNumber = -1;
    if (nextBucketBlockNumber != -1) {
        if (!ReadBlock(m_ReadBlock, nextBucketBlockNumber)) {
            Assert(false, "Invalid block");
            return;
        }
        if (m_ReadBlock->GetRecordsCount() < m_RecordsPerBlock) {
            m_ReadBlock->Append(*record.GetData());
            WriteBlock(m_ReadBlock, nextBucketBlockNumber);
            return;
        }
        previousBucketBlockNumber = nextBucketBlockNumber;
    }

    // Add new overflow block
    m_WriteBlock->Clear();
    memcpy(m_WriteBlock->GetHeader().data(), (const char*)&previousBucketBlockNumber, sizeof(previousBucketBlockNumber));
    m_WriteBlock->Append(*record.GetData());
    AddBlock(m_WriteBlock);

    nextBucketBlockNumber = m_File->GetHead()->GetBlocksCount() - 1;
    m_File->GetHead()->Buckets[bucketHash].blockNumber = nextBucketBlockNumber;
}

void HashRecordManager::Delete(unsigned long long id)
{
    int removedCount = 0;
    unsigned int bucketNumber = hashFunction(id);
    auto schema = GetSchema();

    span<unsigned char> recordToRemove;
    if (SelectSpan(id, &recordToRemove)) {
        Assert(false, "Record not found");
        return;
    }

    auto nextBucketBlockNumber = m_File->GetHead()->Buckets[bucketNumber].blockNumber;
    while (nextBucketBlockNumber != 1) {
        if (!ReadBlock(m_WriteBlock, nextBucketBlockNumber)) {
            Assert(false, "Invalid block");
            return;
        }
        if (m_WriteBlock->GetRecordsCount() > 0)
        {
            break;
        }
        nextBucketBlockNumber = *(unsigned long long*)m_WriteBlock->GetHeader().data();
    }

    m_WriteBlock->MoveToEnd();
    m_WriteBlock->Retreat();
    span<unsigned char> tmpRemovedRecord;
    if (!m_WriteBlock->GetRecordSpan(m_WriteBlock->GetRecordsCount() - 1, &tmpRemovedRecord))
    {
        Assert(false, "Invalid record");
        return;
    }
    m_WriteBlock->Remove();

    memcpy(recordToRemove.data(), tmpRemovedRecord.data(), GetSchema()->GetSize());

    auto blockId = m_NextReadBlockNumber - 1;
    WriteBlock(m_ReadBlock, blockId);
    WriteBlock(m_WriteBlock, nextBucketBlockNumber);
}

int HashRecordManager::DeleteWhereEquals(unsigned int columnId, span<unsigned char> data)
{
    if (columnId != 0) {
        return BaseRecordManager::DeleteWhereEquals(columnId, data);
    }

    auto id = *(unsigned long long*)data.data();
    auto record = Select(id);
    if (record == nullptr) {
        Assert(false, "Record not found");
        return 0;
    }

    Delete(id);

    return 1;
}

FileHead* HashRecordManager::CreateNewFileHead(Schema* schema)
{
    return new HashFileHead(schema);
}

FileWrapper<FileHead>* HashRecordManager::GetFile()
{
    return (FileWrapper<FileHead>*)m_File;
}

void HashRecordManager::DeleteInternal(unsigned long long recordId, unsigned long long blockId, unsigned long long recordNumberInBlock)
{
    Delete(recordId);
}

void HashRecordManager::Reorganize()
{
}
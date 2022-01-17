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
    m_LastQueryBlockAccessCount = 0;
    unsigned int bucketNumber = hashFunction(id);
    auto record = new Record(GetSchema());
    m_NextReadBlockNumber = m_File->GetHead()->Buckets[bucketNumber].blockNumber;
    while (m_NextReadBlockNumber != -1) {
        if (!m_File->GetBlock(m_NextReadBlockNumber, m_ReadBlock)) {
            Assert(false, "Invalid block");
            return nullptr;
        }
        m_LastQueryBlockAccessCount++;
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

vector<Record*> HashRecordManager::SelectWhereEquals(unsigned int columnId, span<unsigned char> data)
{
    if (columnId != 0) {
        return BaseRecordManager::SelectWhereEquals(columnId, data);
    }

    m_LastQueryBlockAccessCount = 0;
    auto records = vector<Record*>();
    auto id = *(unsigned long long*)data.data();
    unsigned int bucketNumber = hashFunction(id);
    auto record = new Record(GetSchema());
    auto nextBucketBlockNumber = m_File->GetHead()->Buckets[bucketNumber].blockNumber;
    while (nextBucketBlockNumber != -1) {
        if (!m_File->GetBlock(nextBucketBlockNumber, m_ReadBlock)) {
            Assert(false, "Invalid block");
            break;
        }
        m_LastQueryBlockAccessCount++;
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
    m_LastQueryBlockAccessCount = 0;

    auto hashRecord = record.As<HashRecord>();
    hashRecord->Id = m_File->GetHead()->NextId;
    m_File->GetHead()->NextId += 1;

    unsigned int bucketHash = hashFunction(hashRecord->Id);
    auto nextBucketBlockNumber = m_File->GetHead()->Buckets[bucketHash].blockNumber;
    unsigned long long previousBucketBlockNumber = -1;
    if (nextBucketBlockNumber != -1) {
        if (!m_File->GetBlock(nextBucketBlockNumber, m_ReadBlock)) {
            Assert(false, "Invalid block");
            return;
        }
        m_LastQueryBlockAccessCount++;
        if (m_ReadBlock->GetRecordsCount() < m_RecordsPerBlock) {
            m_ReadBlock->Append(*record.GetData());
            m_File->WriteBlock(m_ReadBlock, nextBucketBlockNumber);
            return;
        }
        previousBucketBlockNumber = nextBucketBlockNumber;
    }

    // Add new overflow block
    m_WriteBlock->Clear();
    memcpy(m_WriteBlock->GetHeader().data(), (const char*)&previousBucketBlockNumber, sizeof(previousBucketBlockNumber));
    m_WriteBlock->Append(*record.GetData());
    m_File->AddBlock(m_WriteBlock);

    nextBucketBlockNumber = m_File->GetHead()->GetBlocksCount() - 1;
    m_File->GetHead()->Buckets[bucketHash].blockNumber = nextBucketBlockNumber;
}

void HashRecordManager::Delete(unsigned long long id)
{
    int removedCount = 0;
    unsigned int bucketNumber = hashFunction(id);
    auto nextBucketBlockNumber = m_File->GetHead()->Buckets[bucketNumber].blockNumber;
    auto schema = GetSchema();
    auto tmpRemovedRecord = new Record(schema);

    auto record = Select(id);
    if (record == nullptr) {
        Assert(false, "Record not found");
        return;
    }
    auto blockId = m_NextReadBlockNumber;
    auto recordNumberInBlock = m_ReadBlock->GetPosition() - 1;

    while (nextBucketBlockNumber != -1) {
        if (!m_File->GetBlock(nextBucketBlockNumber, m_ReadBlock)) {
            Assert(false, "Invalid block");
            break;
        }
        m_LastQueryBlockAccessCount++;
        m_ReadBlock->MoveToEnd();
        m_ReadBlock->Retreat();
        m_ReadBlock->GetRecord(tmpRemovedRecord->GetData());
        nextBucketBlockNumber = *(unsigned long long*)m_ReadBlock->GetHeader().data();
    }
    m_ReadBlock->Retreat();
    m_ReadBlock->Remove();
    
    memcpy(record->GetData(), tmpRemovedRecord->GetData(), GetSchema()->GetSize());
    m_WriteBlock->Clear();
    m_ReadBlock->MoveToStart();
    auto currentRecord = new Record(schema);
    auto recordCounter = 0;
    while(m_ReadBlock->GetRecord(currentRecord->GetData()))
    {
        auto recordData = currentRecord->GetData();
        if (recordCounter == recordNumberInBlock)
        {
            recordData = record->GetData();
        }
        m_WriteBlock->Append(*recordData);
        recordCounter++;
    }
    m_File->WriteBlock(m_WriteBlock, blockId);
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

void HashRecordManager::DeleteInternal(unsigned long long blockId, unsigned long long recordNumberInBlock)
{

}
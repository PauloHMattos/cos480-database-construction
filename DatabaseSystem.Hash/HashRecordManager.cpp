#include "pch.h"
#include "HashRecordManager.h"
#include "../DatabaseSystem.Core/Assertions.h"
#include "HashFileHead.h"

HashRecordManager::HashRecordManager(size_t blockSize, unsigned int numberOfBuckets) :
    m_File(new FileWrapper<HashFileHead>(blockSize, sizeof(unsigned long long))),
    m_ReadBlock(nullptr),
    m_WriteBlock(nullptr),
    m_NextReadBlockNumber(0),
    m_RecordsPerBlock(0),
    m_NumberOfBuckets(numberOfBuckets)
{
}

void HashRecordManager::Create(string path, Schema* schema)
{
    m_File->NewFile(path, new HashFileHead(schema));
    m_File->GetHead()->SetBucketCount(m_NumberOfBuckets);

    auto schemaSize = GetSchema()->GetSize();
    auto blockLength = m_File->GetBlockSize();
    auto blockContentLength = m_File->GetBlockSize() - sizeof(unsigned int) - sizeof(unsigned long long);
    m_RecordsPerBlock = floor(blockContentLength / schemaSize);

    m_ReadBlock = m_File->CreateBlock();
    m_WriteBlock = m_File->CreateBlock();
}

void HashRecordManager::Open(string path)
{
    m_File->Open(path, new HashFileHead());
    Assert(m_NumberOfBuckets == m_File->GetHead()->Buckets.size(), "Buckets count missmatch");

    auto schemaSize = GetSchema()->GetSize();
    auto blockLength = m_File->GetBlockSize();
    auto blockContentLength = m_File->GetBlockSize() - sizeof(unsigned int) - sizeof(unsigned long long);
    m_RecordsPerBlock = floor(blockContentLength / schemaSize);

    m_ReadBlock = m_File->CreateBlock();
    m_WriteBlock = m_File->CreateBlock();
}

void HashRecordManager::Close()
{
    m_File->Close();
}

Schema* HashRecordManager::GetSchema()
{
    return m_File->GetHead()->GetSchema();
}

unsigned long long HashRecordManager::GetSize()
{
    auto writtenBlocks = m_File->GetBlockSize() * m_File->GetHead()->GetBlocksCount();
    auto recordsToBeWritten = m_WriteBlock->GetRecordsCount() * GetSchema()->GetSize();
    return writtenBlocks + recordsToBeWritten;
}

unsigned int HashRecordManager::hashFunction(unsigned long long key)
{
    return key % m_File->GetHead()->Buckets.capacity();
}

Record* HashRecordManager::Select(unsigned long long id)
{
    unsigned int bucketNumber = hashFunction(id);
    auto record = new Record(GetSchema());
    auto nextBucketBlockNumber = m_File->GetHead()->Buckets[bucketNumber].blockNumber;
    while (nextBucketBlockNumber != -1) {
        if (!m_File->GetBlock(nextBucketBlockNumber, m_ReadBlock)) {
            Assert(false, "Invalid block");
            return nullptr;
        }

        while (m_ReadBlock->GetRecord(record->GetData())) {
            if (record->getId() == id) {
                return record;
            }
        }
        nextBucketBlockNumber = *(unsigned long long*)m_ReadBlock->GetHeader().data();
    }
    return nullptr;
}

// TODO - SelectWhereEquals()


void HashRecordManager::Insert(Record record)
{
    auto hashRecord = record.As<HashRecord>();
    hashRecord->Id = m_File->GetHead()->NextId;
    m_File->GetHead()->NextId += 1;

    unsigned int bucketHash = hashFunction(hashRecord->Id);
    auto nextBucketBlockNumber = m_File->GetHead()->Buckets[bucketHash].blockNumber;
    auto previousBucketBlockNumber = -1;
    while (nextBucketBlockNumber != -1) {
        if (!m_File->GetBlock(nextBucketBlockNumber, m_ReadBlock)) {
            Assert(false, "Invalid block");
            return;
        }

        if (m_ReadBlock->GetRecordsCount() < m_RecordsPerBlock) {
            m_ReadBlock->Append(*record.GetData());
            m_File->WriteBlock(m_ReadBlock, nextBucketBlockNumber);
            return;
        }
        previousBucketBlockNumber = nextBucketBlockNumber;
        nextBucketBlockNumber = *(unsigned long long*)m_ReadBlock->GetHeader().data();
    }

    // Add new overflow block
    m_WriteBlock->Clear();
    nextBucketBlockNumber = -1;
    memcpy(m_WriteBlock->GetHeader().data(), (const char*)&nextBucketBlockNumber, sizeof(nextBucketBlockNumber));
    m_WriteBlock->Append(*record.GetData());
    m_File->AddBlock(m_WriteBlock);

    nextBucketBlockNumber = m_File->GetHead()->GetBlocksCount() - 1;
    if (nextBucketBlockNumber > 0)
    {
        memcpy(m_ReadBlock->GetHeader().data(), (const char*)&nextBucketBlockNumber, sizeof(nextBucketBlockNumber));
        m_File->WriteBlock(m_ReadBlock, previousBucketBlockNumber);
    }
    m_File->GetHead()->Buckets[bucketHash].blockNumber = previousBucketBlockNumber;
}

void HashRecordManager::Delete(unsigned long long id)
{
    //unsigned int bucketNumber = hashFunction(id);

    //Block* currentBlock = m_Buckets[bucketNumber].block;

    //if (currentBlock) {
    //    Record* record = Select(id);
    //    // mark as deleted
    //}
}

void HashRecordManager::MoveToStart()
{
    m_NextReadBlockNumber = 0;
}

bool HashRecordManager::MoveNext(Record* record, unsigned long long& accessedBlocks, unsigned long long& blockId, unsigned long long& recordNumberInBlock)
{
    auto blocksCount = m_File->GetHead()->GetBlocksCount();
    auto initialBlock = m_NextReadBlockNumber;
    if (m_NextReadBlockNumber == 0)
    {
        //did not start to read before
        if (blocksCount > 0)
        {
            ReadNextBlock();
        }
    }

    bool returnVal = GetNextRecordInFile(record);

    if (returnVal)
    {
        recordNumberInBlock = m_ReadBlock->GetPosition() - 1;
        blockId = m_NextReadBlockNumber - 1;
    }
    accessedBlocks = m_NextReadBlockNumber - initialBlock;
    return returnVal;
}

bool HashRecordManager::GetNextRecordInFile(Record* record)
{
    auto recordData = record->GetData();
    auto blocksInFile = m_File->GetHead()->GetBlocksCount();
    while (blocksInFile > 0 && m_NextReadBlockNumber < blocksInFile - 1)
    {
        while (m_ReadBlock->GetRecord(recordData))
        {
            auto recordHeapData = record->As<HashRecord>();
            if (recordHeapData->Id != -1)
            {
                return true;
            }
        }
        ReadNextBlock();
    }
    
    // Not found in the file
    // Look in the write buffer
    if (m_WriteBlock->GetRecordsCount() > 0)
    {

        while (m_WriteBlock->GetPosition() < m_WriteBlock->GetRecordsCount())
        {
            if (m_WriteBlock->GetRecord(recordData))
            {
                // Here we dont have to check for the id. 
                // When we remove records from the write block we dont set the id.
                // Just remove from the list
                return true;
            }
        }
    }
    return false;
}

void HashRecordManager::WriteAndRead()
{
    m_File->AddBlock(m_WriteBlock);
    m_WriteBlock->Clear();
    ReadNextBlock();
}

void HashRecordManager::ReadNextBlock()
{
    m_ReadBlock->Clear();
    m_File->GetBlock(m_NextReadBlockNumber, m_ReadBlock);
    m_ReadBlock->MoveToStart();
    m_NextReadBlockNumber++;
}

void HashRecordManager::DeleteInternal(unsigned long long blockId, unsigned long long recordNumberInBlock)
{

}
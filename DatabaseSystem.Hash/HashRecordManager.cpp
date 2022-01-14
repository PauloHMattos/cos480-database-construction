#include "pch.h"
#include "HashRecordManager.h"
#include "../DatabaseSystem.Core/Assertions.h"
#include "HashFileHead.h"

HashRecordManager::HashRecordManager(size_t blockSize, unsigned int numberOfBuckets) :
    m_File(new File<HashFileHead>(blockSize)),
    m_ReadBlock(nullptr),
    m_WriteBlock(nullptr),
    m_NextReadBlockNumber(0),
    m_RecordsPerBlock(0),
    m_NumberOfBuckets(numberOfBuckets)
{
    m_Buckets.reserve(numberOfBuckets);
    for (size_t i = 0; i < numberOfBuckets; i++)
    {
        auto bucket = Bucket(i);
        m_Buckets.push_back(bucket);
    }
}

void HashRecordManager::Create(string path, Schema* schema)
{
    m_File->NewFile(path, new HashFileHead(schema));

    auto schemaSize = GetSchema()->GetSize();
    auto blockLength = m_File->GetBlockSize();
    auto blockContentLength = m_File->GetBlockSize() - sizeof(unsigned int);
    m_RecordsPerBlock = floor(blockContentLength / schemaSize);

    m_ReadBlock = m_File->CreateBlock();
    m_WriteBlock = m_File->CreateBlock();
}

void HashRecordManager::Open(string path)
{
    m_File->Open(path, new HashFileHead());

    auto schemaSize = GetSchema()->GetSize();
    auto blockLength = m_File->GetBlockSize();
    auto blockContentLength = m_File->GetBlockSize() - sizeof(unsigned int);
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
    return key % m_NumberOfBuckets;
}

Record* HashRecordManager::Select(unsigned long long id)
{
    unsigned int bucketNumber = hashFunction(id);

    auto recordBlock = m_Buckets[bucketNumber].block;

    if (recordBlock) {
        for (unsigned long long key : m_Buckets[bucketNumber].keys) {
            if (key == id) {
                // Itera pelo bloco até achar o registro
                m_NextReadBlockNumber = m_Buckets[bucketNumber].blockNumber;

                m_LastQueryBlockAccessCount = 0;
                unsigned long long accessedBlocks = 0;

                auto schema = GetSchema();
                auto currentRecord = new Record(schema);

                while (MoveNext(currentRecord, accessedBlocks))
                {
                    m_LastQueryBlockAccessCount += accessedBlocks;
                    if (currentRecord->getId() == id) {
                        return currentRecord;
                    }
                }
            }
        }
    }

    return nullptr;
}

void HashRecordManager::Insert(Record record)
{
    auto hashRecord = record.As<HashRecord>();
    hashRecord->Id = m_File->GetHead()->NextId;
    m_File->GetHead()->NextId += 1;

    unsigned int bucketNumberToInsert = hashFunction(hashRecord->Id);

    Block* currentBlock = m_Buckets[bucketNumberToInsert].block;

    // Se o bucket não tem seu bloco corresponde, cria e atualiza
    if (!currentBlock) {
        currentBlock = m_File->CreateBlock();
        m_File->AddBlock(currentBlock);
        currentBlock->Clear();
        m_Buckets[bucketNumberToInsert].block = currentBlock;
        m_Buckets[bucketNumberToInsert].blockNumber = m_File->GetHead()->GetBlocksCount();
    }
    else if (m_Buckets[bucketNumberToInsert].recordsCount >= MAX_RECORDS) {
        // TODO: Overflow bucket
    }

    auto recordData = record.GetData();
    currentBlock->Append(*recordData);
    m_Buckets[bucketNumberToInsert].recordsCount++;
}

void HashRecordManager::Delete(unsigned long long id)
{
    unsigned int bucketNumber = hashFunction(id);

    Block* currentBlock = m_Buckets[bucketNumber].block;

    if (currentBlock) {
        Record* record = Select(id);
        // mark as deleted
    }
}

void HashRecordManager::MoveToStart()
{
    m_NextReadBlockNumber = 0;
}

bool HashRecordManager::MoveNext(Record* record, unsigned long long& accessedBlocks)
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
        else
        {
            //have nothing in file and m_WriteBlock may contain some data
            if (m_WriteBlock->GetRecordsCount() > 0)
            {
                //m_WriteBlock has some records - write it to file and read from there
                WriteAndRead();
            }
            else
            {
                return false;
            }
        }
    }

    auto recordData = record->GetData();
    bool returnVal = m_ReadBlock->GetRecord(recordData);

    if (!returnVal) {
        if (m_NextReadBlockNumber == blocksCount && m_WriteBlock->GetRecordsCount() == 0) {
            // We dont have any more blocs to read from
        }
        else if (m_NextReadBlockNumber == blocksCount && m_WriteBlock->GetRecordsCount() != 0) {
            // We have some records in m_WriteBlock
            // Write it to disk and load to the read block
            WriteAndRead();
            returnVal = m_ReadBlock->GetRecord(recordData);
            if (!returnVal) {
                Assert(false, "Should not reached this line. The are records um the write block");
            }
        }
        else if (m_NextReadBlockNumber < blocksCount) {
            // Reads next block
            ReadNextBlock();
            returnVal = m_ReadBlock->GetRecord(recordData);
        }
        else {
            Assert(false, "Should not reached this line");
        }
    }
    accessedBlocks = m_NextReadBlockNumber - initialBlock;
    return returnVal;
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
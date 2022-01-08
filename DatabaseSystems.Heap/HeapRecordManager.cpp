#include "pch.h"
#include "HeapRecordManager.h"
#include "../DatabaseSystem.Core/Assertions.h"

HeapRecordManager::HeapRecordManager(size_t blockSize) :
    m_File(new File<HeapFileHead>(blockSize)),
    m_ReadBlock(nullptr),
    m_WriteBlock(nullptr),
    m_NextReadBlockNumber(0),
    m_RemovedRecords(list<unsigned long long>()),
    m_RecordsPerBlock(0)
{
}

void HeapRecordManager::Create(string path, Schema schema)
{
    m_File->NewFile(path, new HeapFileHead(schema));

    auto schemaSize = GetSchema().GetSize();
    auto blockLength = m_File->GetBlockSize();
    auto blockContentLength = m_File->GetBlockSize() - sizeof(unsigned int);
    m_RecordsPerBlock = floor(blockContentLength / schemaSize);

    m_ReadBlock = m_File->CreateBlock();
    m_WriteBlock = m_File->CreateBlock();
}

void HeapRecordManager::Open(string path)
{
    m_File->Open(path, new HeapFileHead());

    auto schemaSize = GetSchema().GetSize();
    auto blockLength = m_File->GetBlockSize();
    auto blockContentLength = m_File->GetBlockSize() - sizeof(unsigned int);
    m_RecordsPerBlock = floor(blockContentLength / schemaSize);

    m_ReadBlock = m_File->CreateBlock();
    m_WriteBlock = m_File->CreateBlock();
}

void HeapRecordManager::Close()
{
    m_File->Close();
}

Schema& HeapRecordManager::GetSchema()
{
    return m_File->GetHead()->GetSchema();
}

void HeapRecordManager::Insert(Record record)
{
    auto heapRecord = record.As<HeapRecord>();
    heapRecord->Id = m_File->GetHead()->NextId;
    m_File->GetHead()->NextId += 1;

    auto recordsCount = m_WriteBlock->GetRecordsCount();
    if (recordsCount == m_RecordsPerBlock) {
        m_File->AddBlock(m_WriteBlock);
        m_WriteBlock->Clear();
    }

    auto recordData = record.GetData();
    m_WriteBlock->Append(*recordData);
}

void HeapRecordManager::Delete(unsigned long long id)
{
    m_RemovedRecords.push_back(id);
}

void HeapRecordManager::MoveToStart()
{
    m_NextReadBlockNumber = 0;
}

bool HeapRecordManager::MoveNext(Record* record)
{
    auto blocksCount = m_File->GetHead()->GetBlocksCount();

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
    return returnVal;
}

void HeapRecordManager::WriteAndRead()
{
    m_File->AddBlock(m_WriteBlock);
    m_WriteBlock->Clear();
    ReadNextBlock();
}

void HeapRecordManager::ReadNextBlock()
{
    m_ReadBlock->Clear();
    m_File->GetBlock(m_NextReadBlockNumber, m_ReadBlock);
    m_ReadBlock->MoveToStart();
    m_NextReadBlockNumber++;
}

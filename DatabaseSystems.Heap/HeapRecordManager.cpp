#include "pch.h"
#include "HeapRecordManager.h"
#include "../DatabaseSystem.Core/Assertions.h"

HeapRecordManager::HeapRecordManager(size_t blockSize, int reorderCount) :
    m_File(new FileWrapper<HeapFileHead>(blockSize)),
    m_ReadBlock(nullptr),
    m_WriteBlock(nullptr),
    m_NextReadBlockNumber(0),
    m_RecordsPerBlock(0),
    m_ReorganizeCount(reorderCount)
{
}

void HeapRecordManager::Create(string path, Schema* schema)
{
    m_File->NewFile(path, new HeapFileHead(schema));

    auto schemaSize = GetSchema()->GetSize();
    auto blockLength = m_File->GetBlockSize();
    auto blockContentLength = m_File->GetBlockSize() - sizeof(unsigned int);
    m_RecordsPerBlock = floor(blockContentLength / schemaSize);

    m_ReadBlock = m_File->CreateBlock();
    m_WriteBlock = m_File->CreateBlock();
}

void HeapRecordManager::Open(string path)
{
    m_File->Open(path, new HeapFileHead());

    auto schemaSize = GetSchema()->GetSize();
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

Schema* HeapRecordManager::GetSchema()
{
    return m_File->GetHead()->GetSchema();
}

unsigned long long HeapRecordManager::GetSize()
{
    auto writtenBlocks = m_File->GetBlockSize() * m_File->GetHead()->GetBlocksCount();
    auto recordsToBeWritten = m_WriteBlock->GetRecordsCount() * GetSchema()->GetSize();
    return writtenBlocks + recordsToBeWritten;
}

void HeapRecordManager::Insert(Record record)
{
    auto fileHead = m_File->GetHead();
    auto heapRecord = record.As<HeapRecord>();
    heapRecord->Id = fileHead->NextId;
    fileHead->NextId += 1;

    if (fileHead->RemovedCount > 0)
    {
        auto pointerToRecordToReplace = fileHead->RemovedRecordHead;
        
        m_File->GetBlock(pointerToRecordToReplace.BlockId, m_ReadBlock);

        auto recordToRemovePointer = RecordPointer();

        span<unsigned char> recordToReplace;
        if (!m_ReadBlock->GetRecordSpan(pointerToRecordToReplace.RecordNumberInBlock, &recordToReplace)) {
            Assert(false, "Record to replace not found");
            // Skip and write to the WriteBlock so the record is not lost
            // Can we treat this better?
            goto write_block; 
        }

        auto recordToRemoveHeapData = Record::Cast<HeapRecord>(&recordToReplace);
        fileHead->RemovedRecordHead = recordToRemoveHeapData->NextDeleted;

        memcpy(recordToReplace.data(), record.GetData(), GetSchema()->GetSize());
        
        recordToRemoveHeapData->NextDeleted = m_File->GetHead()->RemovedRecordHead;
        m_File->WriteBlock(m_ReadBlock, pointerToRecordToReplace.BlockId);

        fileHead->RemovedCount -= 1;
        return;
    }

write_block:
    auto recordsCount = m_WriteBlock->GetRecordsCount();
    if (recordsCount == m_RecordsPerBlock) {
        m_File->AddBlock(m_WriteBlock);
        m_WriteBlock->Clear();
    }

    auto recordData = record.GetData();
    m_WriteBlock->Append(*recordData);
}

void HeapRecordManager::MoveToStart()
{
    m_NextReadBlockNumber = 0;
}

bool HeapRecordManager::MoveNext(Record* record, unsigned long long& accessedBlocks, unsigned long long& blockId, unsigned long long& recordNumberInBlock)
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

void HeapRecordManager::DeleteInternal(unsigned long long blockNumber, unsigned long long recordNumberInBlock)
{
    auto fileHead = m_File->GetHead();
    if (blockNumber == fileHead->GetBlocksCount())
    {
        // Record to remove was not written to file
        // Remove it from m_WriteBlock

        Assert(m_WriteBlock->RemoveRecordAt(recordNumberInBlock), "Block was invalid");
    }


    auto recordToRemovePointer = RecordPointer();
    recordToRemovePointer.BlockId = blockNumber;
    recordToRemovePointer.RecordNumberInBlock = recordNumberInBlock;

    if (fileHead->RemovedCount > 0)
    {
        auto pointerToLastRemovedRecord = m_File->GetHead()->RemovedRecordTail;
        m_File->GetBlock(pointerToLastRemovedRecord.BlockId, m_ReadBlock);

        span<unsigned char> lastRemovedRecord;
        if (!m_ReadBlock->GetRecordSpan(pointerToLastRemovedRecord.RecordNumberInBlock, &lastRemovedRecord)) {
            Assert(false, "Oh no!");
            return;
        }

        auto lastRemovedRecordHeapData = Record::Cast<HeapRecord>(&lastRemovedRecord);
        lastRemovedRecordHeapData->NextDeleted = recordToRemovePointer;
        m_File->WriteBlock(m_ReadBlock, pointerToLastRemovedRecord.BlockId);
    }
    else
    {
        fileHead->RemovedRecordHead = recordToRemovePointer;
    }
    fileHead->RemovedRecordTail = recordToRemovePointer;
    fileHead->RemovedCount += 1;


    m_File->GetBlock(blockNumber, m_ReadBlock);
    span<unsigned char> recordToRemove;
    if (!m_ReadBlock->GetRecordSpan(recordNumberInBlock, &recordToRemove)) {
        Assert(false, "Oh no!");
        return;
    }

    // Mark as removed
    auto recordToRemoveHeapData = Record::Cast<HeapRecord>(&recordToRemove);
    recordToRemoveHeapData->Id = -1;
    m_File->WriteBlock(m_ReadBlock, blockNumber);

    if (fileHead->RemovedCount > m_ReorganizeCount) {
        Reorganize();
    }
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
    m_WriteBlock->MoveToStart();
    m_NextReadBlockNumber++;
}

bool HeapRecordManager::GetNextRecordInFile(Record* record)
{
    auto recordData = record->GetData();
    auto blocksInFile = m_File->GetHead()->GetBlocksCount();
    while (blocksInFile > 0 && m_NextReadBlockNumber < blocksInFile - 1)
    {
        while (m_ReadBlock->GetRecord(recordData))
        {
            auto recordHeapData = record->As<HeapRecord>();
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

void HeapRecordManager::Reorganize()
{
    // Write all data to the file
    if (m_WriteBlock->GetRecordsCount() > 0)
    {
        WriteAndRead();
    }

    auto fileHead = m_File->GetHead();
    auto totalBlocksCount = fileHead->GetBlocksCount();
    if (totalBlocksCount == 0)
    {
        return;
    }

    auto pointerToRecordToReplace = fileHead->RemovedRecordHead;
    m_File->GetBlock(pointerToRecordToReplace.BlockId, m_WriteBlock);

    auto currentReadBlock = totalBlocksCount - 1;
    while (fileHead->RemovedCount > 0 && currentReadBlock > 0)
    {
        m_File->GetBlock(currentReadBlock, m_ReadBlock);
        m_ReadBlock->MoveToEnd();

        while (fileHead->RemovedCount > 0 && m_ReadBlock->GetRecordsCount() > 0)
        {
            m_ReadBlock->Retreat();
            span<unsigned char> recordData;
            if (!m_ReadBlock->GetCurrentSpan(&recordData))
            {
                Assert(false, "Oh no 2");
                return;
            }

            span<unsigned char> recordToReplaceData;
            m_WriteBlock->GetRecordSpan(pointerToRecordToReplace.RecordNumberInBlock, &recordToReplaceData);

            auto currentBlock = pointerToRecordToReplace.RecordNumberInBlock;
            pointerToRecordToReplace = Record::Cast<HeapRecord>(&recordToReplaceData)->NextDeleted;

            memcpy(recordToReplaceData.data(), recordData.data(), recordData.size());

            fileHead->RemovedCount--;
            m_ReadBlock->Remove();

            if (currentBlock != pointerToRecordToReplace.BlockId)
            {
                m_File->WriteBlock(m_WriteBlock, currentBlock);
                m_File->GetBlock(pointerToRecordToReplace.BlockId, m_WriteBlock);
            }
        }

        m_File->WriteBlock(m_ReadBlock, currentReadBlock);
        currentReadBlock--;
    }
    fileHead->SetBlocksCount(currentReadBlock);
    m_File->Trim();
}
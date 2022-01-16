#include "pch.h"
#include "HeapRecordManager.h"
#include "../DatabaseSystem.Core/Assertions.h"

HeapRecordManager::HeapRecordManager(size_t blockSize, int reorderCount) :
    BaseRecordManager(),
    m_File(new FileWrapper<HeapFileHead>(blockSize)),
    m_ReorganizeCount(reorderCount)
{
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
        if (!m_ReadBlock->GetRecordSpan(pointerToRecordToReplace.RecordNumberInBlock, &recordToReplace)) 
        {
            Assert(false, "Record to replace not found");
            // Skip and write to the WriteBlock so the record is not lost
            // Can we treat this better?
            goto write_block; 
        }

        auto recordToRemoveHeapData = Record::Cast<HeapRecord>(&recordToReplace);
        fileHead->RemovedRecordHead = recordToRemoveHeapData->NextDeleted;

        memcpy(recordToReplace.data(), record.GetData(), GetSchema()->GetSize());
        
        recordToRemoveHeapData->NextDeleted = fileHead->RemovedRecordHead;
        m_File->WriteBlock(m_ReadBlock, pointerToRecordToReplace.BlockId);

        fileHead->RemovedCount -= 1;
        return;
    }

write_block:
    auto recordsCount = m_WriteBlock->GetRecordsCount();
    if (recordsCount == m_RecordsPerBlock) 
    {
        m_File->AddBlock(m_WriteBlock);
        m_WriteBlock->Clear();
    }

    auto recordData = record.GetData();
    m_WriteBlock->Append(*recordData);
}

FileHead* HeapRecordManager::CreateNewFileHead(Schema* schema)
{
    return new HeapFileHead(schema);
}

FileWrapper<FileHead>* HeapRecordManager::GetFile()
{
    return (FileWrapper<FileHead>*)m_File;
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
        if (!m_ReadBlock->GetRecordSpan(pointerToLastRemovedRecord.RecordNumberInBlock, &lastRemovedRecord)) 
        {
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
    if (!m_ReadBlock->GetRecordSpan(recordNumberInBlock, &recordToRemove)) 
    {
        Assert(false, "Oh no!");
        return;
    }

    // Mark as removed
    auto recordToRemoveHeapData = Record::Cast<HeapRecord>(&recordToRemove);
    recordToRemoveHeapData->Id = -1;
    m_File->WriteBlock(m_ReadBlock, blockNumber);

    if (fileHead->RemovedCount > m_ReorganizeCount) 
    {
        Reorganize();
    }
}

void HeapRecordManager::WriteAndRead()
{
    m_File->AddBlock(m_WriteBlock);
    m_WriteBlock->Clear();
    ReadNextBlock();
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
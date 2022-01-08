#include "pch.h"
#include "OrderedRecordManager.h"
#include "../DatabaseSystem.Core/Assertions.h"

OrderedRecordManager::OrderedRecordManager(size_t blockSize) : m_File(new File<OrderedFileHead>(blockSize)),
                                                               m_ExtensionFile(new File<OrderedFileHead>(blockSize)),
                                                               m_ReadBlock(nullptr),
                                                               m_WriteBlock(nullptr),
                                                               m_NextReadBlockNumber(0),
                                                               m_MaxExtensionFileSize(10),
                                                               m_MaxPercentEmptySpace(0.2),
                                                               m_RecordsPerBlock(0)
{
}

void OrderedRecordManager::Create(string path, Schema schema)
{
  m_File->NewFile(path, new OrderedFileHead(schema));
  auto extension_path = path.append(".extension");
  m_ExtensionFile->NewFile(extension_path, new OrderedFileHead(schema));

  auto schemaSize = GetSchema().GetSize();
  auto blockLength = m_File->GetBlockSize();
  auto blockContentLength = m_File->GetBlockSize() - sizeof(unsigned int);
  m_RecordsPerBlock = floor(blockContentLength / schemaSize);

  m_ReadBlock = m_File->CreateBlock();
  m_WriteBlock = m_ExtensionFile->CreateBlock();
}

void OrderedRecordManager::Open(string path)
{
  m_File->Open(path, new OrderedFileHead());
  auto extension_path = path.append(".extension");
  m_ExtensionFile->Open(extension_path, new OrderedFileHead());

  auto schemaSize = GetSchema().GetSize();
  auto blockLength = m_File->GetBlockSize();
  auto blockContentLength = m_File->GetBlockSize() - sizeof(unsigned int);
  m_RecordsPerBlock = floor(blockContentLength / schemaSize);

  m_ReadBlock = m_File->CreateBlock();
  m_WriteBlock = m_ExtensionFile->CreateBlock();
}

void OrderedRecordManager::Close()
{
  m_File->Close();
  m_ExtensionFile->Close();
}

Schema &OrderedRecordManager::GetSchema()
{
  return m_File->GetHead()->GetSchema();
}

void OrderedRecordManager::Insert(Record record)
{
  // TODO
  // insert into extension file
  // if extension file larger than max size
  // call reorder
}

Record *OrderedRecordManager::Select(unsigned long long id)
{
  // TODO
  // binary search this bitch
}

vector<Record *> OrderedRecordManager::Select(vector<unsigned long long> ids)
{
  // TODO
  // for id in ids
  // select id
}

vector<Record *> OrderedRecordManager::SelectWhereBetween(unsigned int columnId, span<unsigned char> min, span<unsigned char> max)
{
  //TODO
  // check if column is id
  // if it is, use select id
  // if not, use base
}

vector<Record *> SelectWhereEquals(unsigned int columnId, span<unsigned char> data)
{
  // TODO
  // check if column is id
  // if it is, use select id
  // if not, use base
}

void OrderedRecordManager::Delete(unsigned long long id)
{
  // TODO
  // find record for id
  // mark as deleted
  // if empty space in file higher than percent allowed
  // call compress
}

void OrderedRecordManager::MoveToStart()
{
  m_NextReadBlockNumber = 0;
}

bool OrderedRecordManager::MoveNext(Record *record)
{
  auto blocksCount = m_File->GetHead()->GetBlocksCount();

  if (m_NextReadBlockNumber == 0)
  {
    // did not start to read before
    if (blocksCount > 0)
    {
      ReadNextBlock();
    }
    else
    {
      // have nothing in file and m_WriteBlock may contain some data
      if (m_WriteBlock->GetRecordsCount() > 0)
      {
        // m_WriteBlock has some records - write it to file and read from there
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

  if (!returnVal)
  {
    // Reads next block
    blocksCount = m_File->GetHead()->GetBlocksCount();
    if (m_NextReadBlockNumber == blocksCount && m_WriteBlock->GetRecordsCount() == 0)
    {
      // We dont have any more blocks to read from
      // TODO: READ FROM EXTENSION FILE ?
    }
    else if (m_NextReadBlockNumber == blocksCount && m_WriteBlock->GetRecordsCount() != 0)
    {
      // We have some records in m_WriteBlock
      // Write it to disk and load to the read block
      WriteAndRead();
      returnVal = m_ReadBlock->GetRecord(recordData);
      if (!returnVal)
      {
        Assert(false, "Should not have reached this line. There are records in the write block");
      }
    }
    else if (m_NextReadBlockNumber < blocksCount)
    {
      ReadNextBlock();
      returnVal = m_ReadBlock->GetRecord(recordData);
    }
    else
    {
      Assert(false, "Should not have reached this line");
    }
  }
  return returnVal;
}

void OrderedRecordManager::WriteAndRead()
{
  m_ExtensionFile->AddBlock(m_WriteBlock);
  m_WriteBlock->Clear();
  ReadNextBlock();
}

void OrderedRecordManager::ReadNextBlock()
{
  m_ReadBlock->Clear();
  m_File->GetBlock(m_NextReadBlockNumber, m_ReadBlock);
  // TODO ?
  // maybe read from extension file ?
  m_ReadBlock->MoveToStart();
  m_NextReadBlockNumber++;
}

void OrderedRecordManager::Reorder()
{
  // TODO
  // insert blocks from m_ExtensionFile into m_File, reordering
}

void OrderedRecordManager::Compress()
{
  // TODO
  // remove empty spaces from m_File
}
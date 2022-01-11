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
                                                               m_RecordsPerBlock(0), 
                                                               m_UsingExtensionAsMain(false)
{
}

void OrderedRecordManager::Create(string path, Schema *schema)
{
  m_File->NewFile(path, new OrderedFileHead(schema));
  auto extension_path = path.append(".extension");
  m_ExtensionFile->NewFile(extension_path, new OrderedFileHead(schema));

  auto schemaSize = GetSchema()->GetSize();
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

  auto schemaSize = GetSchema()->GetSize();
  auto blockLength = m_File->GetBlockSize();
  auto blockContentLength = m_File->GetBlockSize() - sizeof(unsigned int);
  m_RecordsPerBlock = floor(blockContentLength / schemaSize);

  m_ReadBlock = m_File->CreateBlock();
  m_WriteBlock = m_ExtensionFile->CreateBlock();
}

void OrderedRecordManager::Close()
{
  if (m_ExtensionFile->GetHead()->GetBlocksCount() > 0) {
    //Reorder();
      MemoryReorder();
  }
  if (m_UsingExtensionAsMain) {
      SwitchFilesHard();
  }
  m_File->Close();
  m_ExtensionFile->Close();
}

Schema *OrderedRecordManager::GetSchema()
{
  return m_File->GetHead()->GetSchema();
}

unsigned long long OrderedRecordManager::GetSize()
{
  auto writtenBlocks = m_File->GetBlockSize() * m_File->GetHead()->GetBlocksCount();
  auto extensionBlocks = m_ExtensionFile->GetBlockSize() * m_ExtensionFile->GetHead()->GetBlocksCount();
  auto recordsWritten = m_WriteBlock->GetRecordsCount() * GetSchema()->GetSize();
  return writtenBlocks + extensionBlocks + recordsWritten;
}

void OrderedRecordManager::Insert(Record record)
{
    auto orderedRecord = record.As<OrderedRecord>();
    orderedRecord->Id = m_ExtensionFile->GetHead()->NextId;
    m_ExtensionFile->GetHead()
        ->NextId += 1;

    auto recordsCount = m_WriteBlock->GetRecordsCount();
    if (recordsCount == m_RecordsPerBlock)
    {
        m_ExtensionFile->AddBlock(m_WriteBlock);
        m_WriteBlock->Clear();

        auto blocksCount = m_ExtensionFile->GetHead()->GetBlocksCount();
        if (blocksCount == m_MaxExtensionFileSize)
        {
            // if Extension File has reached max size, call Reorder
            //Reorder();
            MemoryReorder();
        }
    }

    auto recordData = record.GetData();
    m_WriteBlock->Append(*recordData);
}

Record *OrderedRecordManager::Select(unsigned long long id)
{
  // TODO
  // binary search this bitch
    return BaseRecordManager::Select(id);
}

vector<Record *> OrderedRecordManager::Select(vector<unsigned long long> ids)
{
  // TODO
  // for id in ids
  // select id
    return BaseRecordManager::Select(ids);
}

vector<Record *> OrderedRecordManager::SelectWhereBetween(unsigned int columnId, span<unsigned char> min, span<unsigned char> max)
{
  //TODO
  // check if column is id
  // if it is, use select id
  // if not, use base
    return BaseRecordManager::SelectWhereBetween(columnId, min, max);

}

vector<Record *> OrderedRecordManager::SelectWhereEquals(unsigned int columnId, span<unsigned char> data)
{
  // TODO
  // check if column is id
  // if it is, use select id
  // if not, use base
    return BaseRecordManager::SelectWhereEquals(columnId, data);
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

bool OrderedRecordManager::MoveNext(Record *record, unsigned long long &accessedBlocks)
{
  auto blocksCount = m_File->GetHead()->GetBlocksCount();
  auto initialBlock = m_NextReadBlockNumber;

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
  accessedBlocks = m_NextReadBlockNumber - initialBlock;
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

bool GetRecord(Block *block, Record *record)
{
  auto recordData = record->GetData();
  return block->GetRecord(recordData);
}

bool GetRecord(Block *block, Record *record, unsigned int offset)
{
  auto recordData = record->GetData();
  return block->GetRecordAt(offset, recordData);
}

void OrderedRecordManager::Reorder()
{
  auto schema = GetSchema();
  Record record = Record(schema);
  auto recordSize = schema->GetSize();
  // TODO
  // insert blocks from m_ExtensionFile into m_File, reordering

  // Sort blocks from m_ExtensionFile and insert them into m_File
  Block *block = m_ExtensionFile->CreateBlock();
  auto blocksCount = m_ExtensionFile->GetHead()->GetBlocksCount();

  unsigned long long blockId;
  for (blockId = 0; blockId < blocksCount; blockId++)
  {
    auto blockRecords = vector<Record>();
    block->Clear();
    m_ExtensionFile->GetBlock(blockId, block);
    block->MoveToStart();
    while (GetRecord(block, &record))
    {
      blockRecords.push_back(record);
    }

    sort(blockRecords.begin(), blockRecords.end(), OrderedRecordManager::RecordComparer);

    // Write sorted data into block and add block into m_File
    block->Clear();
    for (auto record : blockRecords)
    {
      auto recordData = record.GetData();
      block->Append(*recordData);
    }
    m_File->AddBlock(block);
  }

  // Sort all blocks in m_File and write to m_ExtensionFile
  auto inputBuffer = vector<Record>();
  auto outputBuffer = vector<Record>();

  m_ExtensionFile->SeekHead();

  unsigned int recordPointer = 0;
  while (recordPointer < m_RecordsPerBlock)
  {
    // Get one record from each block
    blocksCount = m_File->GetHead()->GetBlocksCount();
    for (blockId = 0; blockId < blocksCount; blockId++)
    {
      block->Clear();
      m_File->GetBlock(blockId, block);
      if (!GetRecord(block, &record, recordPointer))
        continue;
      inputBuffer.push_back(record);
    }

    // Sort input buffer and copy into output buffer
    sort(inputBuffer.begin(), inputBuffer.end(), RecordComparer);
    outputBuffer.insert(outputBuffer.end(), inputBuffer.begin(), inputBuffer.end());

    if (outputBuffer.size() >= m_RecordsPerBlock)
    {
      // Copy output buffer into m_ExtensionFile
      auto outBlock = m_ExtensionFile->CreateBlock();
      for (int i = 0; i < m_RecordsPerBlock; i++)
      {
          record = outputBuffer[i];
          auto recordData = record.GetData();
          outBlock->Append(*recordData);
      }
      m_ExtensionFile->AddBlock(outBlock);
      outputBuffer = vector(outputBuffer.begin() + m_RecordsPerBlock, outputBuffer.end());
    }
    inputBuffer = vector<Record>();
    recordPointer++;
  }

  SwitchFilesSoft();
  
  // let n be the amount of blocks to merge
  // Read the first RecordsPerBlock / n+1 records of each sorted block into n input buffers in main memory and allocate the remaining block space for an output buffer.
  // CREATE N INPUT BUFFERS
  // CREATE 1 OUTPUT BUFFER
  // READ ith BLOCK FROM MAIN FILE
  // WRITE RecordsPerBlock / n+1 RECORDS INTO ith INPUT BUFFER
  // MERGE SORT INPUT BUFFERS INTO OUTPUT BUFFER
  // WHEN OUTPUT BUFFER FULL, WRITE TO TMP FILE
  // WHEN ith INPUT BUFFER FULL, READ FROM ith BLOCK
  // WHEN MAIN FILE IS EMPTY, COPY TMP FILE INTO FILE

  // Perform a merge and store the result in the output buffer. Whenever the output buffer fills, write it to the final sorted file and empty it. Whenever any of the input buffers empties, fill it with the next RecordsPerBlock / n+1 records of its associated sorted block until no more data from the chunk is available. This is the key step that makes external merge sort work externally—because the merge algorithm only makes one pass sequentially through each of the blocks, each block does not have to be loaded completely; rather, sequential parts of the chunk can be loaded as needed.
}

void OrderedRecordManager::MemoryReorder() 
{
    auto records = vector<Record>();
    auto schema = GetSchema();
    auto record = Record(schema);

    auto blocksCount = m_File->GetHead()->GetBlocksCount();

    // Read all records from m_File
    unsigned long long blockId;
    for (blockId = 0; blockId < blocksCount; blockId++)
    {
        m_ReadBlock->Clear();
        m_File->GetBlock(blockId, m_ReadBlock);
        m_ReadBlock->MoveToStart();
        while (GetRecord(m_ReadBlock, &record))
        {
            records.push_back(record);
        }
    }

    blocksCount = m_ExtensionFile->GetHead()->GetBlocksCount();
    // Read all records from m_ExtensionFile
    for (blockId = 0; blockId < blocksCount; blockId++)
    {
        m_ReadBlock->Clear();
        m_ExtensionFile->GetBlock(blockId, m_ReadBlock);
        m_ReadBlock->MoveToStart();
        while (GetRecord(m_ReadBlock, &record))
        {
            records.push_back(record);
        }
    }
    m_ExtensionFile->SeekHead();

    // Sort all records
    sort(records.begin(), records.end(), RecordComparer);

    // Write back to m_File
    m_File->SeekHead();
    m_WriteBlock->Clear();
    for (auto &record : records) {
        auto recordData = record.GetData();
        m_WriteBlock->Append(*recordData);

        auto recordsCount = m_WriteBlock->GetRecordsCount();
        if (recordsCount == m_RecordsPerBlock) {
            m_File->AddBlock(m_WriteBlock);
            m_WriteBlock->Clear();
        }
    }
}

void OrderedRecordManager::Compress()
{
  // TODO
  // remove empty spaces from m_File
}

void OrderedRecordManager::SwitchFilesSoft()
{
    auto mainFilePath = m_File->GetPath();
    auto mainFileHead = m_File->GetHead();

    auto extensionFilePath = m_ExtensionFile->GetPath();
    auto extensionFileHead = m_ExtensionFile->GetHead();

    m_File->UpdatePath(extensionFilePath, extensionFileHead);

    m_ExtensionFile->UpdatePath(mainFilePath, mainFileHead);
    m_ExtensionFile->SeekHead();

    m_UsingExtensionAsMain = !m_UsingExtensionAsMain;
}

void OrderedRecordManager::SwitchFilesHard()
{
    // TODO
    // copy content of extension file into main file

}

bool OrderedRecordManager::RecordComparer(Record a, Record b)
{
  return (a.getId() < b.getId());
}
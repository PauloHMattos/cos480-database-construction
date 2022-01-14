#pragma once
#include "../DatabaseSystem.Core/BaseRecordManager.h"
#include "../DatabaseSystem.Core/Record.h"
#include "../DatabaseSystem.Core/File.h"
#include "../DatabaseSystem.Core/Block.h"
#include "OrderedFileHead.h"

typedef bool (*EvalFunctionType)(int, unsigned long long, unsigned long long&, unsigned long long&);

/*
  Ordered, ou arquivo sequencial ordenado, com registros de tamanho fixo,
  inserção de novos em um arquivo de extensão e posterior reordenação, e remoção baseada em marcação dos registros removidos,
  que deverão ser reorganizados posteriormente para compressão do arquivo.
*/
class OrderedRecordManager : public BaseRecordManager
{
public:
  OrderedRecordManager(size_t blockSize);
  virtual void Create(string path, Schema *schema) override;
  virtual void Create(string path, Schema* schema, unsigned int orderByColumnId) override;
  virtual void Open(string path) override;
  virtual void Close() override;

  // Inherited via BaseRecordManager
  virtual Schema *GetSchema() override;
  virtual unsigned long long GetSize() override;
  virtual void Insert(Record record) override;
  virtual Record *Select(unsigned long long id) override;
  virtual vector<Record *> SelectWhereBetween(unsigned int columnId, span<unsigned char> min, span<unsigned char> max) override;
  virtual vector<Record *> SelectWhereEquals(unsigned int columnId, span<unsigned char> data) override;
  virtual void Delete(unsigned long long id) override;

protected:
  // Inherited via BaseRecordManager
  virtual void MoveToStart() override;
  virtual bool MoveNext(Record *record, unsigned long long &accessedBlocks) override;

private:
  File<OrderedFileHead> *m_File;
  File<OrderedFileHead> *m_ExtensionFile;
  Block *m_ReadBlock;
  Block *m_WriteBlock;
  unsigned long long m_NextReadBlockNumber;
  unsigned long long m_RecordsPerBlock;
  unsigned int m_OrderedByColumnId;
  unsigned long long m_MaxExtensionFileSize;
  float m_MaxPercentEmptySpace;
  bool m_UsingExtensionAsMain;

  bool MovePrev(Record* record, unsigned long long& accessedBlocks);
  void MoveToExtension();
  void WriteAndRead();
  void ReadNextBlock();
  void ReadPrevBlock();
  void ReadBlock(unsigned long long blockId);
  void Reorder();  // inserts records from extension file into main file, reordering
  void MemoryReorder(); // reads all records from main file and extension file into memory and reorders, for debugging
  void Compress(); // removes records marked as deleted from the file, compressing the empty space
  void SwitchFilesSoft(); // switches m_File and m_ExtensionFile paths
  void SwitchFilesHard(); // copy m_ExtensionFile data into m_File
  Record* BinarySearch(span<unsigned char> target, EvalFunctionType evalFunc, unsigned long long& accessedBlocks);

  struct OrderedRecord
  {
    unsigned long long Id;
  };
};
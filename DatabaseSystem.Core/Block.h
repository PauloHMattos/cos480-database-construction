#pragma once

#include "DoubleList.h"

class Block
{
public:
	Block(unsigned int blockSize, unsigned int recordSize);

	void Load(span<unsigned char> data);
	void Flush(span<unsigned char> data);

	void Clear();
	void Append(span<unsigned char> data);

	void MoveToStart();
	void MoveToEnd();
	void Retreat();
	void Remove();

	unsigned int GetRecordsCount();
	bool GetRecord(vector<unsigned char>* record);
	bool GetRecordSpan(unsigned long long recordNumberInBlock, span<unsigned char>* record);
	bool GetCurrentSpan(span<unsigned char>* record);
	int GetPosition();
	bool RemoveRecordAt(unsigned long long recordNumber);
	size_t GetCurrentLengthInBytes();
	unsigned int GetBlockSize() const;

	struct BlockRecordMap {
		unsigned int RecordStart;
		unsigned int RecordLength;
	};

	unsigned int GetFinishRecordMap() const;
	unsigned int GetStartRecords() const;
	unsigned int GetCurrRecordSize();

	// Var length record
	long long GetVarRecordId(unsigned int recordNumber);
	unsigned int GetDataRecordsCount();
	unsigned int GetRecordPos(unsigned int recordNumber);
	unsigned int GetRecordLength(unsigned int recordNumber);
	void UpdateRecordMapPos(unsigned int recordNumber, unsigned int newPos);
	void ShiftBytes(vector<unsigned char>& buffer, unsigned int currPos, unsigned int length, unsigned int bytesShifted, bool isLeftDir);
	void RemoveRecordMap(vector<unsigned char>& buffer, unsigned int recordNumber);
	void UpdateStartRecordPos(unsigned int newPos);
	void UpdateRecordsCount(unsigned int newCount);
	void PrintRecordMapper();
	unsigned int GetRecordsCountVar();

private:
	unsigned int m_RecordSize;
	vector<unsigned char> m_BlockData;
	DoubleList<span<unsigned char>> m_Records;
	size_t m_CurrentLengthInBytes;

	unsigned int m_RecordsCount;
	unsigned int m_FinishRecordMap;
	unsigned int m_StartRecords;
	unsigned int m_MetaDataCount;
};

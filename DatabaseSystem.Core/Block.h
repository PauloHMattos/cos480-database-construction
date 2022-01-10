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
	int GetRecordsCount();
	bool GetRecord(vector<unsigned char>* record);

private:
	unsigned int m_RecordSize;
	vector<unsigned char> m_BlockData;
	DoubleList<span<unsigned char>> m_Records;
	size_t m_CurrentLengthInBytes;
};

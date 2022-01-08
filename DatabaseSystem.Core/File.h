#pragma once
#include "Block.h"
#include "FileHead.h"

template<derived_from<FileHead> TFileHead>
class File
{
public:
	File(size_t blockSize) : 
		m_FileHead(nullptr), 
		m_BlockSize(blockSize) 
	{
	}

	void Open(string path, TFileHead* head)
	{
		m_FilePath = path;
		m_Stream.open(path);
		m_Stream.seekg(0, ios::beg);
		m_Stream.seekp(0, ios::beg);
		m_FileHead = head;
		head->Deserialize(m_Stream);
		m_FirstBlockPos = (streamoff)m_Stream.tellg() - 1;
	}

	void Close()
	{
		m_Stream.seekp(0, ios::beg);
		m_FileHead->Serialize(m_Stream);
		m_Stream.flush();
		m_Stream.close();
	}

	void NewFile(string path, TFileHead* head)
	{
		m_FilePath = path;
		m_FileHead = head;
		m_Stream.open(path);
		m_Stream.seekg(0, ios::beg);
		m_Stream.seekp(0, ios::beg);
		head->Serialize(m_Stream);
		m_Stream.flush();
		m_FirstBlockPos = m_Stream.tellp();
	}

	bool GetBlock(unsigned int blockId, Block* destination)
	{
		vector<unsigned char> tempBuffer(m_BlockSize);

		m_Stream.clear();
		m_Stream.seekg(m_FirstBlockPos + streamoff(m_BlockSize * blockId), ios::beg);

		m_Stream.read((char*)tempBuffer.data(), m_BlockSize);
		destination->Load(tempBuffer);
		return true;
	}


	void AddBlock(Block* block) {
		auto blockNumber = m_FileHead->GetBlocksCount();
		m_FileHead->SetBlocksCount(blockNumber + 1);

		vector<unsigned char> tempBuffer(m_BlockSize);
		block->Flush(tempBuffer);

		m_Stream.clear();
		m_Stream.seekp(m_FirstBlockPos + streamoff(m_BlockSize * blockNumber), ios::beg);

		m_Stream.write((const char*)tempBuffer.data(), m_BlockSize);
		m_Stream.flush();
	}

	Block* CreateBlock()
	{
		return new Block(m_BlockSize, m_FileHead->GetSchema().GetSize());
	}

	TFileHead* GetHead()
	{
		return m_FileHead;
	}

	size_t GetBlockSize()
	{
		return m_BlockSize;
	}

protected:
	string m_FilePath;
	size_t m_BlockSize;
	fstream m_Stream;
	TFileHead* m_FileHead;
	streamoff m_FirstBlockPos;
};
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
		m_Stream.open(path, ios::in | ios::out | ios::binary);
		m_Stream.seekg(0);
		m_FileHead = head;
		head->Deserialize(m_Stream);
	}

	void Close()
	{
		m_Stream.close();
	}

	void NewFile(string path, TFileHead* head)
	{
		m_FilePath = path;
		m_FileHead = head;
		m_Stream.open(path, fstream::out | fstream::in | fstream::binary);
		m_Stream.seekg(0, ios::beg);
		head->Serialize(m_Stream);
		m_FirstBlockPos = m_Stream.tellg();

	}

	bool GetBlock(unsigned int blockId, Block* destination)
	{
		vector<unsigned char> tempBuffer(m_BlockSize);

		m_Stream.seekg(m_FirstBlockPos + (streampos)(m_BlockSize * blockId));
		m_Stream.read((char*)tempBuffer.data(), tempBuffer.size());

		destination->Load(tempBuffer);
		return true;
	}

	void AddBlock(Block* block) {
		m_FileHead->SetBlocksCount(m_FileHead->GetBlocksCount() + 1);
		m_Stream.seekg(0, ios::beg);
		m_FileHead->Serialize(m_Stream);

		vector<unsigned char> tempBuffer(m_BlockSize);
		block->Flush(tempBuffer);
		m_Stream.seekg(0, ios::end);
		m_Stream.write((const char*)tempBuffer.data(), tempBuffer.size());
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
	streampos m_FirstBlockPos;
};
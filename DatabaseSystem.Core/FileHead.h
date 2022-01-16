#pragma once
#include "Schema.h"
#include "Serializeble.h"

class FileHead : public Serializable
{
public:
	unsigned long long NextId;
	Schema* GetSchema();

	unsigned int GetBlocksCount()
	{
		return m_BlocksCount;
	}

	void SetBlocksCount(unsigned int value)
	{
		m_BlocksCount = value;
	}

	// Inherited via Serializable
	virtual void Serialize(iostream& dst) override;
	virtual void Deserialize(iostream& src) override;

protected:
	Schema* m_Schema;
	unsigned int m_BlocksCount;
};
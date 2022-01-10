#pragma once
#include "Schema.h"
#include "Serializeble.h"

class FileHead : public Serializable
{
public:
	Schema* GetSchema();

	unsigned int GetBlocksCount() {
		return m_BlocksCount;
	}

	void SetBlocksCount(unsigned int value) {
		m_BlocksCount = value;
	}

	//unsigned int getRecordsCount();
	//void setRecordsCount(unsigned int newCount);
	//unsigned int getRecordStartOffset();

	// Inherited via Serializable
	virtual void Serialize(iostream& dst) override;
	virtual void Deserialize(iostream& src) override;

protected:
	Schema* m_Schema;
	unsigned int m_BlocksCount;
	//unsigned int m_RecordsStartOffset;
};
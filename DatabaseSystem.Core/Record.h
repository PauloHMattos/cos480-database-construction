#pragma once
#include "Schema.h"

class Record
{
public:
	Record(Schema& schema);
	//void CopyData(Record* record);
	vector<unsigned char>* GetData();
	void SetData(size_t index, unsigned char value);

	unsigned long long getId() const;

	template <typename T>
	T* As()
	{
		return (T*)m_Data.data();
	}
	
private:
	Schema& m_Schema;
	vector<unsigned char> m_Data;
};
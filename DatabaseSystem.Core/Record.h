#pragma once
#include "Schema.h"

class Record
{
public:
	Record(Schema* schema);
	vector<unsigned char>* GetData();
	void SetData(size_t index, unsigned char value);

	unsigned long long getId() const;

	template <typename T>
	static T* Cast(span<unsigned char>* data)
	{
		return (T*)data->data();
	}

	template <typename T>
	T* As()
	{
		return (T*)m_Data.data();
	}

	void Write(ostream& out);
	static vector<Record> LoadFromCsv(Schema& schema, string path, unsigned long long lines);

private:
	Schema* m_Schema;
	vector<unsigned char> m_Data;
};
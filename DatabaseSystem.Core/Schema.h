#pragma once
#include "Column.h"
#include "Serializeble.h"

class Schema : public Serializable
{
public:
	Schema();
	void AddColumn(string name, ColumnType type, unsigned int arraySize = 0);
	unsigned int GetSize() const;
	Column GetColumn(unsigned int columnId);
	unsigned int GetColumnId(string columnName) const;
	span<unsigned char> GetValue(vector<unsigned char>* data, unsigned int columnId);

	// Inherited via Serializable
	void Serialize(iostream& dst) override;
	void Deserialize(iostream& src) override;

private:
	vector<Column> m_Columns;
	unsigned int m_Size;
};
